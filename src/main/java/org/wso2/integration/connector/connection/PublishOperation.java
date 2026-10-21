/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.integration.connector.connection;

import com.solacesystems.jcsmp.JCSMPSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.hooks.Hooks;

import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;

/**
 * The operation record of a deadline bounded broker interaction (guide 3.4, design 7.10).
 * Every transition runs under this record's monitor and no broker call happens inside it.
 */
public final class PublishOperation {

    private static final Log log = LogFactory.getLog(PublishOperation.class);

    enum State { REGISTERED, ATTACHED, COMPLETED, FAILED, EXPIRED }

    enum Stage { BEFORE_SEND, CONNECTING, SENDING, SENT }

    final String id = UUID.randomUUID().toString();
    final long deadline;
    final SolaceLogicalConnection lc;
    private State state = State.REGISTERED;
    // The exact resource attached: a pooled holder, a dedicated session's candidate, or the shared reply session.
    private SolaceConnection conn;
    private JCSMPSession session;
    private RequestReplySession rr;
    // C.5 publish.startToConfirmation
    final long registeredNanos = System.nanoTime();
    // Guarded by this record's monitor, so expire() reads it in the same step as its own transition (A51).
    private Stage stage = Stage.BEFORE_SEND;
    private ScheduledFuture<?> timer;

    private PublishOperation(SolaceLogicalConnection lc, long deadline) {
        this.lc = lc;
        this.deadline = deadline;
    }

    /** A budget from now. */
    public static PublishOperation register(SolaceLogicalConnection lc, long ackTimeoutMillis) throws SolaceFault {
        return registerUntil(lc, System.currentTimeMillis() + ackTimeoutMillis);
    }

    /**
     * An absolute deadline the caller already holds. The record keeps that instant and only the scheduling delay
     * is computed from it, so a caller delayed between its own clock read and this call cannot move its deadline.
     * Called before anything can block, as the operation's first statement inside its try.
     */
    public static PublishOperation registerUntil(SolaceLogicalConnection lc, long deadlineMillis) throws SolaceFault {
        if (lc.closed) {
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + lc.name + "' is closed");
        }
        PublishOperation op = new PublishOperation(lc, deadlineMillis);
        try {
            // A spent deadline fires at once.
            op.timer = lc.watchdog.schedule(op::expire, Math.max(0, deadlineMillis - System.currentTimeMillis()),
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // The last detach already shut the watchdog down. Nothing is attached, nothing to undo.
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + lc.name + "' is closed", e);
        }
        if (log.isDebugEnabled()) {
            log.debug("solace.publish: operation " + op.id + " registered with the watchdog, deadline in "
                    + (deadlineMillis - System.currentTimeMillis()) + " ms");
        }
        return op;
    }

    public long remaining() {
        return deadline - System.currentTimeMillis();
    }

    /**
     * The operation thread moves the stage. Refused once the record is EXPIRED, so no send starts after an expiry
     * (A51). The claim of SENDING also reads the absolute deadline, so a spent budget never becomes a send when the
     * watchdog is late (A67).
     */
    public synchronized boolean enterStage(Stage next) {
        if (state == State.EXPIRED) {
            return false;
        }
        if (next == Stage.SENDING && System.currentTimeMillis() >= deadline) {
            return false;
        }
        stage = next;
        return true;
    }

    public synchronized Stage stage() {
        return stage;
    }

    /** REGISTERED or ATTACHED to ATTACHED. */
    public synchronized boolean attach(SolaceConnection pooled) {
        if (state == State.EXPIRED) {
            return false;
        }
        state = State.ATTACHED;
        conn = pooled;
        session = pooled.session;
        return true;
    }

    /** The rebuild inside ensureSession, or a dedicated session's first connect. */
    public synchronized boolean attachSession(JCSMPSession created) {
        if (state == State.EXPIRED) {
            return false;
        }
        session = created;
        return true;
    }

    /**
     * The transfer decision for a dedicated session's first connect and for an attempt record's hand over (3.3).
     * It is atomic with expire() and with the deadline. True means the record gives the session up before it is
     * exposed, so a later timer closes nothing. False means the session is not transferred and the closer owns it:
     * either expire() already captured it, or the budget is spent while the timer has not fired yet, and the expiry
     * disposition is taken here under the same monitor, so a late connect is never published.
     */
    public boolean transferSession() {
        JCSMPSession captured;
        SolaceConnection holder;
        synchronized (this) {
            if (state != State.EXPIRED && System.currentTimeMillis() < deadline) {
                session = null;
                return true;
            }
            if (state == State.EXPIRED) {
                // expire() captured the session and submitted its close.
                return false;
            }
            // The delayed callback case: the same transition expire() makes, so the callback finds a terminal record.
            state = State.EXPIRED;
            captured = session;
            holder = conn;
            session = null;
            Hooks.get().point("op.afterExpireLocked", this);
            if (holder != null && captured != null && holder.session == captured) {
                holder.failed = true;
            }
        }
        if (captured != null) {
            lc.counters.publishExpired.increment();
            lc.closeOnCloser(captured);
        }
        return false;
    }

    /** 3.6: the dedicated session has no pool object. */
    public synchronized boolean attach(RequestReplySession replySession) {
        if (state == State.EXPIRED) {
            return false;
        }
        state = State.ATTACHED;
        rr = replySession;
        session = replySession.session;
        return true;
    }

    public synchronized boolean complete() {
        if (state == State.EXPIRED) {
            return false;
        }
        state = State.COMPLETED;
        return true;
    }

    public synchronized void fail() {
        if (state != State.EXPIRED && state != State.COMPLETED) {
            state = State.FAILED;
        }
    }

    /**
     * The one exit protocol, called in the operation's finally on every path. A non terminal record becomes FAILED
     * so a timer callback that is already running finds a terminal record and touches nothing. EXPIRED and
     * COMPLETED keep their disposition.
     */
    public void finish() {
        synchronized (this) {
            if (state == State.REGISTERED || state == State.ATTACHED) {
                state = State.FAILED;
            }
        }
        if (timer != null) {
            timer.cancel(false);
        }
    }

    public synchronized boolean isExpired() {
        return state == State.EXPIRED;
    }

    /** A publish whose record expired in a wait returns its holder healthy from the operation's finally (A51). */
    public synchronized boolean expiredWith(JCSMPSession candidate) {
        return state == State.EXPIRED && session == candidate && stage != Stage.BEFORE_SEND;
    }

    /**
     * One idempotent transition: the watchdog timer, the caller's own timeout, an interrupt, an exception inside
     * send(). The captured session is closed off the watchdog, so a blocked connect or send throws.
     */
    public void expire() {
        Hooks.get().point("op.beforeExpire", this);
        JCSMPSession captured;
        SolaceConnection holder;
        synchronized (this) {
            if (state == State.COMPLETED || state == State.FAILED || state == State.EXPIRED) {
                return;
            }
            state = State.EXPIRED;
            // The session is captured only when the stage says it is in use. At BEFORE_SEND the caller is in the
            // pool wait, the permit wait or the producer lock wait, and its own timed wait faults with nothing
            // sent (A51).
            boolean inUse = stage != Stage.BEFORE_SEND;
            captured = inUse ? session : null;
            holder = inUse ? conn : null;
            Hooks.get().point("op.afterExpireLocked", this);
            // In the same step as EXPIRED, so no observer sees one without the other.
            if (holder != null && captured != null && holder.session == captured) {
                holder.failed = true;
            }
            // The request reply session is rebuilt at its next use.
            if (rr != null && captured != null && rr.session == captured) {
                rr.failed = true;
            }
        }
        if (captured != null) {
            lc.counters.publishExpired.increment();
            lc.closeOnCloser(captured);
        }
    }
}
