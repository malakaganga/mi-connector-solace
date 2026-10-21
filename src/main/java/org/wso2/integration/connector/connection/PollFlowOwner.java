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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.settlement.SettlementResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.wso2.integration.connector.constants.SolaceConstants.ACK_MODE_CLIENT;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.OWNER_CALL_TIMEOUT_MS;
import static org.wso2.integration.connector.constants.SolaceConstants.POLL_SLICE_MS;
import static org.wso2.integration.connector.constants.SolaceConstants.REASON_ALREADY_DECIDED;
import static org.wso2.integration.connector.constants.SolaceConstants.REASON_STALE_DELIVERY;

/**
 * One cached poll flow and the one thread that touches it (guide 3.5, design 7.10). Every receive, settle, expiry
 * and close of the flow runs on this owner thread. Callers hand in PollRequest and OwnerCommand objects through
 * the inbox and wait on them. The owner is an attached owner of the logical connection from its constructor to
 * closeNow() or discard().
 */
public final class PollFlowOwner implements Runnable {

    private static final Log log = LogFactory.getLog(PollFlowOwner.class);

    final PollSession ps;
    final SolaceLogicalConnection lc;
    final String key;
    final String queue;
    final String selector;
    final int generation;
    final CountDownLatch ready = new CountDownLatch(1);
    private FlowReceiver flow;
    // PollRequest objects and OwnerCommand objects: settle, expiry, close and idle check.
    private final LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    // QUEUED requests in arrival order. Owner thread only.
    private final ArrayDeque<PollRequest> requests = new ArrayDeque<>();
    // Every OUTSTANDING lease handed out by this owner. Owner thread only.
    private final Set<PollLease> liveLeases = new HashSet<>();
    // The short lock over closing, busy and liveLeaseCount.
    private final Object state = new Object();
    private boolean closing = false;
    // Requests accepted by ownerFor and not yet finished: queued, being served or dropped. The one busy count
    // that eviction reads.
    private int busy = 0;
    // Mirror of liveLeases.size(), written under state by the owner.
    private int liveLeaseCount = 0;
    volatile long lastUsedAt = System.currentTimeMillis();
    volatile boolean closed = false;
    private ScheduledFuture<?> idleTimer;

    PollFlowOwner(PollSession ps, String key, String queue, String selector) {
        this.ps = ps;
        this.lc = ps.lc;
        this.key = key;
        this.queue = queue;
        this.selector = selector;
        this.generation = ps.generation;
        // Inside a published PollSession the count is at least 1, so this attach cannot fail (3.3).
        lc.attach(this);
    }

    /** ownerFor: the caller is counted from here until finishRequest(). */
    boolean accept() {
        synchronized (state) {
            if (closing) {
                return false;
            }
            busy++;
            return true;
        }
    }

    /** Exactly once per accepted request, at the release points guide 3.5 lists. */
    private void finishRequest() {
        synchronized (state) {
            busy--;
        }
    }

    /**
     * The evictor's decision and the admission stop are one step. busy covers queued and served requests, so no
     * active request is ever evicted.
     */
    boolean beginEvictionIfIdle() {
        synchronized (state) {
            if (closing || busy > 0 || liveLeaseCount > 0 || !inbox.isEmpty()) {
                return false;
            }
            closing = true;
            return true;
        }
    }

    /** An owner that was never published: its thread never started, it holds no request and no lease. */
    void discard() {
        synchronized (state) {
            closing = true;
            closed = true;
        }
        try {
            if (flow != null) {
                flow.close();
            }
        } catch (Exception e) {
            log.debug("SolaceConnector [" + lc.name + "] closing the unpublished poll flow " + queue + " failed", e);
        }
        lc.detach(this);
    }

    /** Operation thread, outside ps.flows (ownerFor). */
    void bind() throws JCSMPException {
        ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
        flowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queue));
        flowProperties.setStartState(true);
        // Client acknowledgement always. AUTO is the owner's own acknowledgement after the hand off. A window of 1
        // bounds the prefetch an idle flow retains (T24-I).
        flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        flowProperties.setTransportWindowSize(1);
        if (selector != null) {
            flowProperties.setSelector(selector);
        }
        // The session's negotiation, the 1.6 algorithm against requireSettlementOutcomes.
        List<XMLMessage.Outcome> requiredOutcomes = new ArrayList<>();
        if (ps.failedSupported) {
            requiredOutcomes.add(XMLMessage.Outcome.FAILED);
        }
        if (ps.rejectedSupported) {
            requiredOutcomes.add(XMLMessage.Outcome.REJECTED);
        }
        if (!requiredOutcomes.isEmpty()) {
            flowProperties.addRequiredSettlementOutcomes(requiredOutcomes.toArray(new XMLMessage.Outcome[0]));
        }
        EndpointProperties endpointProperties = new EndpointProperties();
        endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        // Null listener: synchronous receive on this owner (Appendix A row 6). A FLOW_DOWN or a FLOW_RECONNECTING
        // submits the owner's close, every other event is DEBUG (A54).
        flow = ps.session.createFlow(null, flowProperties, endpointProperties, new PollFlowEventHandler(this));
    }

    void startThread() {
        Thread ownerThread = new Thread(this,
                "solace-conn-" + lc.name + "-poll-" + lc.pollOwnerIndex.incrementAndGet());
        ownerThread.setDaemon(true);
        ownerThread.start();
    }

    /**
     * The owner loop: commands first, then the oldest request still QUEUED. Every exit goes through closeNow(),
     * which releases the attachment and completes or expires every outstanding request and lease.
     */
    @Override
    public void run() {
        while (!closed) {
            try {
                // Settle, expiry, idle and close commands. New requests are appended.
                drainInbox(0);
                // Expired requests are dropped and never consume.
                PollRequest request = nextQueued();
                if (request == null) {
                    // Wait for work, at most one slice, so a command never waits longer.
                    drainInbox(POLL_SLICE_MS);
                    continue;
                }
                lastUsedAt = System.currentTimeMillis();
                try {
                    serve(request);
                } finally {
                    // A request still QUEUED after serve is expired so its caller wakes, and busy is released once.
                    expire(request);
                    finishRequest();
                    lastUsedAt = System.currentTimeMillis();
                }
                // Never armed after a close processed inside serve.
                if (!closed) {
                    armIdleTimer();
                }
            } catch (JCSMPException e) {
                log.warn("SolaceConnector [" + lc.name + "] poll flow " + queue + " receive failed; closing the flow: "
                        + e.getMessage());
                closeNow("receive failed: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeNow("interrupted");
            } catch (RuntimeException e) {
                // 0.6 rule 10: the owner's terminal cleanup. A failure in any step must not leave an attached owner
                // with live leases and a queued close that no thread will run.
                log.error("SolaceConnector [" + lc.name + "] poll flow " + queue + " owner failed; closing the flow", e);
                closeNow("owner failed: " + e);
            } catch (Error e) {
                // 0.6 rule 10: accounting, then rethrow.
                closeNow("owner died: " + e);
                throw e;
            }
        }
    }

    /** Owner thread. The request is QUEUED and counted busy. */
    private void serve(PollRequest request) throws JCSMPException, InterruptedException {
        while (true) {
            long remaining = request.deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                // Served to its deadline: nothing received, nothing consumed.
                expire(request);
                return;
            }
            // One slice. A settle or an expiry waits at most one slice behind an empty receive (design 7.10).
            BytesXMLMessage message = flow.receive((int) Math.min(POLL_SLICE_MS, remaining));
            if (message == null) {
                // Between slices the settle, expiry, idle and close commands run. New requests queue behind this one.
                drainInbox(0);
                if (closed) {
                    // A close processed between slices. The caller wakes with nothing and run's finally releases busy.
                    expire(request);
                    return;
                }
                continue;
            }
            // Every received message gets the owner's lease before the hand off, whatever the ackMode.
            PollLease lease = new PollLease(this, generation, message,
                    System.currentTimeMillis() + lc.settings.pollAckTimeoutMillis);
            // Registered on the owner first: the flow is active for eviction from here.
            liveLeases.add(lease);
            synchronized (state) {
                liveLeaseCount = liveLeases.size();
            }
            boolean client = ACK_MODE_CLIENT.equals(request.ackMode);
            if (client) {
                // The watchdog only submits. The expiry runs on this thread. The watchdog is alive while this owner
                // is attached (3.3), so schedule() cannot reject here or in armIdleTimer().
                lease.timer = lc.watchdog.schedule(
                        () -> submit(OwnerCommand.of(() -> {
                            expireLease(lease);
                            return null;
                        }, lease)),
                        lc.settings.pollAckTimeoutMillis, MILLISECONDS);
            }
            Hooks.get().point("poll.beforeHandoff", this, request);
            boolean handed;
            // One atomic hand off: result and lease together, or nothing.
            synchronized (request) {
                handed = request.state == PollRequest.QUEUED;
                if (handed) {
                    request.state = PollRequest.DONE;
                    request.result = message;
                    // The AUTO caller gets no lease to stash.
                    request.lease = client ? lease : null;
                    Hooks.get().point("poll.afterHandoffBeforeNotify", this, request);
                    request.notifyAll();
                }
            }
            if (handed) {
                // AUTO: acknowledged on the owner right after the hand off. A JCSMPException propagates to run(),
                // which closes the flow.
                if (!client) {
                    settleOnOwner(lease, XMLMessage.Outcome.ACCEPTED, "ackMode=AUTO");
                }
                return;
            }
            // The caller expired meanwhile: never deliver to it (T24-M, G15).
            lc.counters.pollLateReceive.increment();
            log.warn("SolaceConnector [" + lc.name + "] polled message " + message.getMessageId()
                    + " received after its caller's timeout on flow " + queue + "; settling FAILED");
            // FAILED when supported, otherwise retain and recycle: the flow closes and the broker redelivers.
            settleOnOwner(lease, XMLMessage.Outcome.FAILED, "late receive for an expired caller");
            return;
        }
    }

    /** Idempotent. A DONE request is untouched. */
    private void expire(PollRequest request) {
        synchronized (request) {
            if (request.state == PollRequest.QUEUED) {
                request.state = PollRequest.EXPIRED;
                request.notifyAll();
            }
        }
    }

    /** busy is not decremented here. A request stays busy from accept() to finishRequest(). */
    private void drainInbox(long waitMillis) throws InterruptedException {
        Object item = inbox.poll(waitMillis, MILLISECONDS);
        while (item != null) {
            if (item instanceof PollRequest) {
                requests.addLast((PollRequest) item);
            } else if (item instanceof OwnerCommand) {
                runOnOwner((OwnerCommand<?>) item);
            }
            item = inbox.poll();
        }
    }

    private void runOnOwner(OwnerCommand<?> command) {
        if (command.isCancelled()) {
            return;
        }
        command.run();
        // The caller reported UNKNOWN. The lease state is whatever settleOnOwner left, SETTLED or EXPIRED, and
        // there is no second transition.
        if (command.claim.get() == OwnerCommand.EXECUTING && command.isCancelled()) {
            log.warn("SolaceConnector [" + lc.name + "] a settlement on poll flow " + queue
                    + " completed after its caller stopped waiting (result discarded; lease state "
                    + (command.lease == null ? "none" : PollLease.stateName(command.lease.state)) + ")");
        }
    }

    /** An expired queued request never consumes. It is finished here (T24-L). */
    private PollRequest nextQueued() {
        PollRequest request;
        while ((request = requests.pollFirst()) != null) {
            synchronized (request) {
                if (request.state == PollRequest.QUEUED) {
                    return request;
                }
            }
            finishRequest();
        }
        return null;
    }

    /**
     * Any thread: enqueue a command and wait for it on this owner. The lease is the one the command settles, or
     * null for the close, idle and expiry commands.
     */
    <T> T call(Callable<T> body, PollLease lease, long waitMillis) throws SolaceFault {
        OwnerCommand<T> command = OwnerCommand.of(body, lease);
        synchronized (state) {
            if (closing || closed) {
                throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, "poll flow " + queue + " is closed");
            }
            inbox.offer(command);
        }
        try {
            return command.get(waitMillis, MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            // The same rule for a timeout and an interrupt (G7).
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // The atomic claim: true means the body will refuse to execute whatever FutureTask.run() decided,
            // false means execution is claimed.
            boolean neverRuns = command.cancelBeforeRun();
            // The FutureTask side: a queued command is dropped by runOnOwner, a running one finishes on the owner.
            command.cancel(false);
            if (!neverRuns) {
                // The owner finishes it normally and logs the orphaned result.
                throw new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                        "the settlement had started on the poll flow owner when the caller stopped waiting; its outcome is unknown to the caller",
                        null, true);
            }
            // Exact: the claim is CANCELLED, the body cannot run.
            throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                    "the settlement was cancelled before it ran; the lease stays outstanding until its expiry");
        } catch (CancellationException e) {
            throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                    "poll flow " + queue + " closed before the command ran");
        } catch (ExecutionException e) {
            // A JCSMPException thrown by settleOnOwner (3.2).
            throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, e.getCause().getMessage(), e.getCause(),
                    false);
        }
    }

    /** A PollRequest from poll, or an OwnerCommand from the watchdog or retire. */
    public void submit(Object item) {
        synchronized (state) {
            if (!closed) {
                inbox.offer(item);
                return;
            }
            // The command's caller reports POLL_ERROR "closed before the command ran".
            if (item instanceof Future) {
                ((Future<?>) item).cancel(false);
            }
            // Accepted by ownerFor, refused here: the caller wakes with nothing and its busy count is released.
            if (item instanceof PollRequest) {
                expire((PollRequest) item);
                finishRequest();
            }
        }
    }

    void submitClose(String why) {
        submit(OwnerCommand.of(() -> {
            closeNow(why);
            return null;
        }, null));
    }

    /** Operation thread. Reads volatile fields only, never the owner's structures. */
    public SettlementResult settle(PollLease lease, XMLMessage.Outcome outcome) throws SolaceFault {
        String outcomeName = outcome == XMLMessage.Outcome.ACCEPTED ? "ACK" : outcome.name();
        if (gone(lease)) {
            return stale(lease, outcomeName);
        }
        // A fast answer. The owner repeats the check in its own order.
        if (lease.state != PollLease.OUTSTANDING) {
            return SettlementResult.notSettled(outcomeName, REASON_ALREADY_DECIDED, -1);
        }
        try {
            return call(() -> settleOnOwner(lease, outcome, "settled by operation"), lease, OWNER_CALL_TIMEOUT_MS);
        } catch (SolaceFault fault) {
            // A close that landed between the checks above and the command, refused or cancelled before it ran, is
            // the "flow already gone" case of 3.2: STALE_DELIVERY, not a fault. UNKNOWN and a JCSMP failure stay
            // faults.
            if (FAULT_POLL_ERROR.equals(fault.name) && gone(lease)) {
                return stale(lease, outcomeName);
            }
            throw fault;
        }
    }

    private boolean gone(PollLease lease) {
        return closed || ps.retired || lease.state == PollLease.STALE;
    }

    private SettlementResult stale(PollLease lease, String outcomeName) {
        log.warn("SolaceConnector [" + lc.name + "] late settlement of polled message " + lease.message.getMessageId()
                + " from a closed poll flow (" + queue + "); the broker redelivers it");
        return SettlementResult.notSettled(outcomeName, REASON_STALE_DELIVERY, -1);
    }

    /**
     * Owner thread only. There is no caller gone branch: the broker action completes whether or not the caller is
     * still waiting (G7).
     */
    private SettlementResult settleOnOwner(PollLease lease, XMLMessage.Outcome outcome, String why)
            throws JCSMPException {
        String outcomeName = outcome == XMLMessage.Outcome.ACCEPTED ? "ACK" : outcome.name();
        // Dropped by a close before this command ran.
        if (lease.state == PollLease.STALE) {
            return SettlementResult.notSettled(outcomeName, REASON_STALE_DELIVERY, -1);
        }
        // Settled or expired first: one winner (T24-E).
        if (lease.state != PollLease.OUTSTANDING) {
            return SettlementResult.notSettled(outcomeName, REASON_ALREADY_DECIDED, -1);
        }
        boolean supported = outcome == XMLMessage.Outcome.ACCEPTED
                || (outcome == XMLMessage.Outcome.FAILED && ps.failedSupported)
                || (outcome == XMLMessage.Outcome.REJECTED && ps.rejectedSupported);
        if (!supported) {
            // The delivery is retained and this one flow recycled so the broker redelivers it. Reported as settled,
            // as the inbound's degraded path is.
            retainAndRecycle(lease, why + ": outcome " + outcome + " is not supported by the broker");
            return SettlementResult.direct(outcomeName, lease.message.getAckMessageId());
        }
        try {
            if (outcome == XMLMessage.Outcome.ACCEPTED) {
                lease.message.ackMessage();
            } else {
                lease.message.settle(outcome);
            }
        } catch (JCSMPException e) {
            log.error("SolaceConnector [" + lc.name + "] settling polled message " + lease.message.getMessageId() + " "
                    + outcomeName + " failed on flow " + queue + ": " + e.getMessage());
            // The flow is recycled so the broker redelivers. The caller faults POLL_ERROR 700108 (3.2). A caller
            // that already left sees the orphaned result WARN.
            terminalise(lease, PollLease.EXPIRED);
            closeNow("settle failed: " + e.getMessage());
            throw e;
        }
        terminalise(lease, PollLease.SETTLED);
        return SettlementResult.direct(outcomeName, lease.message.getAckMessageId());
    }

    /** Owner thread. Idempotent for a lease already removed. */
    private void terminalise(PollLease lease, int finalState) {
        lease.state = finalState;
        if (lease.timer != null) {
            lease.timer.cancel(false);
        }
        liveLeases.remove(lease);
        synchronized (state) {
            liveLeaseCount = liveLeases.size();
        }
    }

    /** Owner thread, submitted by the watchdog timer. */
    private void expireLease(PollLease lease) {
        // Settled or dropped first: one winner (T24-E).
        if (lease.state != PollLease.OUTSTANDING) {
            return;
        }
        lc.counters.pollLeaseExpired.increment();
        if (!ps.failedSupported) {
            // T24 F. The line names the outcome that happens here (A56).
            log.warn("SolaceConnector [" + lc.name + "] polled message " + lease.message.getMessageId()
                    + " was not settled within pollAckTimeout " + lc.settings.pollAckTimeoutMillis
                    + " ms; retained (no FAILED support), the flow is recycled");
            retainAndRecycle(lease, "lease expired without FAILED support");
            return;
        }
        log.warn("SolaceConnector [" + lc.name + "] polled message " + lease.message.getMessageId()
                + " was not settled within pollAckTimeout " + lc.settings.pollAckTimeoutMillis + " ms; settling FAILED");
        try {
            lease.message.settle(XMLMessage.Outcome.FAILED);
            terminalise(lease, PollLease.EXPIRED);
        } catch (JCSMPException e) {
            log.error("SolaceConnector [" + lc.name + "] settling expired lease " + lease.message.getMessageId()
                    + " FAILED failed on flow " + queue + ": " + e.getMessage());
            terminalise(lease, PollLease.EXPIRED);
            closeNow("settle failed: " + e.getMessage());
        }
    }

    /** Closing the flow releases the delivery to the broker. */
    private void retainAndRecycle(PollLease lease, String why) {
        terminalise(lease, PollLease.EXPIRED);
        closeNow("retained delivery: " + why);
    }

    /** The watchdog only submits. The decision runs on the owner between slices. */
    private void armIdleTimer() {
        if (idleTimer != null) {
            idleTimer.cancel(false);
        }
        idleTimer = lc.watchdog.schedule(
                () -> submit(OwnerCommand.of(() -> {
                    idleCheck();
                    return null;
                }, null)),
                lc.settings.pollFlowIdleTimeoutMillis, MILLISECONDS);
    }

    /**
     * The same predicate as eviction. busy covers the request being served, so a long receive is never closed
     * under it.
     */
    private void idleCheck() {
        long idle = System.currentTimeMillis() - lastUsedAt;
        if (idle >= lc.settings.pollFlowIdleTimeoutMillis && beginEvictionIfIdle()) {
            closeNow("idle for " + idle + " ms");
        } else if (!closed) {
            armIdleTimer();
        }
    }

    /** Owner thread only, from a command or run's catch. Every outcome below is final. */
    private void closeNow(String why) {
        // Admission stops here: accept(), submit() and call() refuse from now on, so the inbox drain below sees
        // everything.
        synchronized (state) {
            if (closed) {
                return;
            }
            closing = true;
            closed = true;
        }
        // The next ownerFor binds a fresh owner.
        synchronized (ps.flows) {
            ps.flows.remove(key, this);
        }
        if (idleTimer != null) {
            idleTimer.cancel(false);
        }
        // A later settle reports STALE_DELIVERY.
        for (PollLease lease : new ArrayList<>(liveLeases)) {
            log.warn("SolaceConnector [" + lc.name + "] polled message " + lease.message.getMessageId()
                    + " left outstanding by the closing flow " + queue + "; the broker redelivers it");
            terminalise(lease, PollLease.STALE);
        }
        // Every queued request: the caller wakes with nothing, busy released once per request.
        PollRequest queued;
        while ((queued = requests.pollFirst()) != null) {
            expire(queued);
            finishRequest();
        }
        // A cancelled settle reports POLL_ERROR "closed before the command ran" to its caller.
        Object item;
        while ((item = inbox.poll()) != null) {
            if (item instanceof PollRequest) {
                expire((PollRequest) item);
                finishRequest();
            } else if (item instanceof OwnerCommand) {
                ((OwnerCommand<?>) item).cancel(false);
            }
        }
        // Releases every unacknowledged delivery to the broker.
        try {
            if (flow != null) {
                flow.close();
            }
        } catch (Exception e) {
            log.debug("SolaceConnector [" + lc.name + "] closing poll flow " + queue + " failed", e);
        }
        lc.detach(this);
        log.info("SolaceConnector [" + lc.name + "] poll flow " + queue + (selector != null ? " [" + selector + "]" : "")
                + " closed (" + why + ")");
    }

    /**
     * A FutureTask with an atomic execution claim, the poll owner's instance of the 3.7 command shape. The body
     * executes only if it wins the claim, and cancelBeforeRun() is the caller's side of the same claim, so the
     * caller learns exactly whether the body can still run. The broker call stays outside the claim.
     */
    static final class OwnerCommand<T> extends FutureTask<T> {

        static final int QUEUED = 0;
        static final int EXECUTING = 1;
        static final int CANCELLED = 2;

        final AtomicInteger claim;
        final PollLease lease;

        private OwnerCommand(Callable<T> callable, AtomicInteger claim, PollLease lease) {
            super(callable);
            this.claim = claim;
            this.lease = lease;
        }

        static <T> OwnerCommand<T> of(Callable<T> body, PollLease lease) {
            AtomicInteger claim = new AtomicInteger(QUEUED);
            return new OwnerCommand<>(() -> {
                if (!claim.compareAndSet(QUEUED, EXECUTING)) {
                    return null;
                }
                return body.call();
            }, claim, lease);
        }

        boolean cancelBeforeRun() {
            return claim.compareAndSet(QUEUED, CANCELLED);
        }
    }
}
