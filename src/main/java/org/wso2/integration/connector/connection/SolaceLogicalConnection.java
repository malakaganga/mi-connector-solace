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

import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SessionEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.libraries.model.Library;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.utils.ClientNames;
import org.wso2.integration.connector.utils.NamedThreadFactory;
import org.wso2.integration.connector.utils.OAuthTokenSource;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.wso2.integration.connector.constants.SolaceConstants.CONNECTOR_NAME;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;

/**
 * One per connection name: the JCSMP Context, the watchdog, the closer, the dedicated sessions, the counters
 * and the client name counter. Created by SolaceConfigConnector.connect() through
 * LogicalConnections.create(), closed when the connection is undeployed or redeployed.
 */
public final class SolaceLogicalConnection {

    private static final Log log = LogFactory.getLog(SolaceLogicalConnection.class);

    public final String name;
    final String nodeId;
    // host, vpn, credentials, TLS, retries, window. CLIENT_NAME is set per session.
    final JCSMPProperties baseProperties;
    public final ConnectionSettings settings;
    // One Context per logical connection.
    final Context context;
    // Timers only, never a JCSMP call.
    final ScheduledThreadPoolExecutor watchdog;
    // The only connector thread that blocks on closeSession().
    final ThreadPoolExecutor closer;
    // The connect threads. A connect runs here under the operation's deadline, because JCSMP's connect() does not
    // notice a closeSession() while it waits on the transport or the login reply.
    final ExecutorService connects;
    // The connect retries of the template, JCSMP's default 0 when absent. A physical attempt connects once, so the
    // retries bound the attempts of the recovery loop instead.
    final int connectRetries;
    private final AtomicBoolean retriesLogged = new AtomicBoolean();
    public final ConnectionCounters counters = new ConnectionCounters();
    // 1, 2, ... assigned to pooled SolaceConnection objects at creation.
    final AtomicInteger pooledIndex = new AtomicInteger();
    // 1, 2, ... the n of each poll owner thread name.
    final AtomicInteger pollOwnerIndex = new AtomicInteger();
    final OAuthTokenSource tokenSource;
    // Guards the dedicated session fields below. It is also the lifecycle monitor: close() stops admission under it,
    // and every final session publication, pooled or dedicated, runs under it. The order is this monitor, then a
    // holder's publication monitor, then an operation record's monitor, never the reverse.
    private final Object dedicated = new Object();
    // Created lazily.
    private PollSession pollSession;
    private RequestReplySession rrSession;
    // The creation attempt in progress, null when none (guarded by dedicated). A caller that finds one waits on it
    // under its own deadline instead of being refused.
    private CompletableFuture<PollSession> pollCreation;
    private CompletableFuture<RequestReplySession> rrCreation;
    // The physical connect attempt in progress of each dedicated session, null when none (guarded by dedicated).
    // It outlives a creator whose budget ended, and the next creator joins it.
    private ConnectAttempt rrAttempt;
    private ConnectAttempt pollAttempt;
    // The close of a replaced dedicated session, null when none (guarded by dedicated). Until it finished no new
    // candidate of that role starts, so a role keeps at most one physical session besides its published one.
    private CompletableFuture<Void> rrRetiring;
    private CompletableFuture<Void> pollRetiring;
    // Guarded by dedicated. PollSession's generation number.
    int pollGeneration;
    // The pooled physical sessions that are connecting, installed or being closed, those of holders the pool
    // already destroyed included. Guarded by pooledSlots, which is never held while another monitor is taken. The
    // dedicated sessions keep their own reservations and are not counted.
    private final Object pooledSlots = new Object();
    private int pooledSessions;
    // Owner tokens of the two creation reservations (the attachment table).
    static final Object POLL_CREATION = new Object();
    static final Object RR_CREATION = new Object();
    // The attached owner count. SEALED once the last owner detached after close().
    private final AtomicInteger attached = new AtomicInteger();
    private static final int SEALED = Integer.MIN_VALUE;
    private final AtomicBoolean closedOnce = new AtomicBoolean();
    public volatile boolean closed = false;
    // The library this class was loaded from, and the configuration that names the deployed one.
    final ClassLoader ownLoader = SolaceLogicalConnection.class.getClassLoader();
    final SynapseConfiguration configuration;
    private final AtomicBoolean retiredOnce = new AtomicBoolean();

    /** LogicalConnections.create() only, under the per name lock. No broker work. */
    SolaceLogicalConnection(String name, MessageContext messageContext) {
        this.name = name;
        this.nodeId = ClientNames.nodeId().id;
        this.configuration = messageContext.getConfiguration();
        this.baseProperties = SolaceUtils.buildJCSMPProperties(messageContext);
        this.connectRetries = ((JCSMPChannelProperties) baseProperties.getProperty(
                JCSMPProperties.CLIENT_CHANNEL_PROPERTIES)).getConnectRetries();
        // parse() cannot fail here. connect() validated the same template before calling create().
        this.settings = ConnectionSettings.parse(messageContext);
        this.context = JCSMPFactory.onlyInstance().createContext(new ContextProperties().setName("solace-conn-" + name));
        this.watchdog = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("solace-conn-" + name + "-watchdog", true));
        watchdog.setRemoveOnCancelPolicy(true);
        this.closer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("solace-conn-" + name + "-closer", true));
        this.connects = Executors.newCachedThreadPool(new NamedThreadFactory("solace-conn-" + name + "-connect", true));
        // The static token of the template, if any, is the fallback.
        this.tokenSource = settings.oauth2TokenFile != null
                ? new OAuthTokenSource(settings.oauth2TokenFile,
                        baseProperties.getStringProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN), name)
                : null;
        // The last statement: the connection's own attachment, released by close().
        attach(this);
    }

    /** Any operation thread. Creates, but does not connect, a session on this Context with the name for the role. */
    JCSMPSession createSession(String role, SessionEventHandler handler) throws JCSMPException {
        if (closed) {
            throw new JCSMPException("logical connection '" + name + "' is closed");
        }
        JCSMPProperties sessionProperties = (JCSMPProperties) baseProperties.clone();
        String clientName = ClientNames.connector(settings.clientName, nodeId, name, role, settings.clientNameStrict);
        if (clientName != null) {
            sessionProperties.setProperty(JCSMPProperties.CLIENT_NAME, clientName);
        }
        if (tokenSource != null) {
            sessionProperties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, tokenSource.current());
        }
        JCSMPChannelProperties channelProperties = (JCSMPChannelProperties)
                sessionProperties.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        if (settings.keepAliveIntervalMillis >= 0) {
            channelProperties.setKeepAliveIntervalInMillis((int) settings.keepAliveIntervalMillis);
        }
        if (settings.keepAliveLimit >= 0) {
            channelProperties.setKeepAliveLimit(settings.keepAliveLimit);
        }
        // A physical attempt connects once. The configured retries bound the recovery loop instead.
        channelProperties.setConnectRetries(0);
        channelProperties.setConnectRetriesPerHost(0);
        if (retriesLogged.compareAndSet(false, true)) {
            log.info("SolaceConnector [" + name + "] connect retries " + connectRetries
                    + " apply to the recovery loop, each physical attempt connects once");
        }
        return JCSMPFactory.onlyInstance().createSession(sessionProperties, context, handler);
    }

    /**
     * One physical connect attempt of a holder, its reservation. A holder keeps at most one at a time under
     * its publication monitor, taken before the connect is submitted. It ends in one of two ways: the session is
     * published, or it goes to its single cleanup owner, and then the reservation is held until the close itself
     * finished, not only until it was queued.
     */
    static final class ConnectAttempt {

        final JCSMPSession session;
        final PooledSessionEventHandler handler;
        // Read before the connect starts: every method of the JCSMP session is synchronized, so a read while
        // connect() holds the monitor would wait for the connect itself.
        final String clientName;
        // The candidate of a dedicated session, the RequestReplySession or the PollSession built for this attempt.
        final Object candidate;
        // The holder's publication monitor.
        final Object monitor;
        // The cleanup owner: it closes the session and completes when the close finished.
        private final Supplier<CompletableFuture<Void>> cleanupAction;
        // Clears the holder's reservation. Runs under monitor once the close finished.
        private final Consumer<ConnectAttempt> onCleaned;
        // Completes when the attempt ended: published, or cleaned and its reservation cleared.
        final CompletableFuture<Void> ended = new CompletableFuture<>();
        Future<?> connect;
        // The callers waiting on the connect. Guarded by monitor.
        int waiters;
        // The connect ended. Guarded by monitor.
        boolean done;
        // A caller took the connected session to prepare and publish it. Guarded by monitor.
        boolean claimed;
        // The connect failure the waiters rethrow. Guarded by monitor.
        JCSMPException failure;
        // A DOWN_ERROR of this candidate before its publication, written by the holder's one failure transition
        // under monitor and read by the publication under the same monitor.
        boolean candidateFailed;
        // Non null once the session went to its cleanup owner. Guarded by monitor.
        CompletableFuture<Void> cleanup;

        ConnectAttempt(JCSMPSession session, PooledSessionEventHandler handler, Object candidate, Object monitor,
                       Supplier<CompletableFuture<Void>> cleanupAction, Consumer<ConnectAttempt> onCleaned) {
            this.session = session;
            this.handler = handler;
            this.clientName = String.valueOf(session.getProperty(JCSMPProperties.CLIENT_NAME));
            this.candidate = candidate;
            this.monitor = monitor;
            this.cleanupAction = cleanupAction;
            this.onCleaned = onCleaned;
        }

        /** Under monitor. The session goes to its cleanup owner once. The reservation stays until the close finished. */
        void startCleanup() {
            if (cleanup != null) {
                return;
            }
            cleanup = cleanupAction.get();
            cleanup.whenComplete((ignored, error) -> {
                synchronized (monitor) {
                    onCleaned.accept(this);
                }
                ended.complete(null);
            });
        }

        boolean inCleanup() {
            return cleanup != null;
        }

        /** Under monitor. The attempt became the holder's session. */
        void published() {
            ended.complete(null);
        }
    }

    /**
     * Holder thread, under the holder's publication monitor with no attempt reserved. Submits the connect of the
     * candidate and returns the attempt the holder stores as its reservation. The connect thread connects
     * once, createSession set the JCSMP retries to 0, and registers the reconnect guard. When the connect
     * fails, or succeeds after every caller gave up, the task itself hands the session to its cleanup owner, so the
     * closer never waits inside a running connect. The reservation is cleared only when that close finished.
     */
    ConnectAttempt startAttempt(JCSMPSession candidate, PooledSessionEventHandler handler, Object dedicatedCandidate,
                                Object monitor, Supplier<CompletableFuture<Void>> cleanupAction,
                                Consumer<ConnectAttempt> onCleaned) throws SolaceFault {
        ConnectAttempt attempt = new ConnectAttempt(candidate, handler, dedicatedCandidate, monitor, cleanupAction,
                onCleaned);
        try {
            attempt.connect = connects.submit(() -> connectTask(attempt));
        } catch (RejectedExecutionException e) {
            // The logical connection is closed. The candidate has an owner on this exit too.
            closeOnCloser(candidate);
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + name + "' is closed", e);
        }
        return attempt;
    }

    /** Connect thread. */
    private void connectTask(ConnectAttempt attempt) {
        JCSMPException failure = null;
        boolean connected = false;
        try {
            attempt.session.connect();
            attempt.handler.registerGuard();
            connected = true;
        } catch (JCSMPException e) {
            failure = e;
        } finally {
            synchronized (attempt.monitor) {
                attempt.done = true;
                attempt.failure = failure;
                // Failed, or connected with no caller left to publish it: the session goes to its cleanup owner
                // after the connect ended, never during it.
                if (!connected || (attempt.waiters == 0 && !attempt.claimed)) {
                    attempt.startCleanup();
                }
            }
        }
    }

    /**
     * Holder thread, outside the monitor, counted as a waiter of the attempt. Waits for the connect under the
     * record's remaining budget and claims the connected session. The claim is the right to prepare it, not its
     * publication: the deadline, the close of the logical connection and a candidate failure are decided at the
     * final publication, in one transition. A caller whose budget ends records EXPIRED and leaves the attempt
     * running for the next caller. A failed connect is thrown to the caller as its own failure.
     */
    void awaitAttempt(ConnectAttempt attempt, PublishOperation op) throws JCSMPException, SolaceFault {
        try {
            attempt.connect.get(Math.max(0, op.remaining()), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            leaveAttempt(attempt);
            // The caller's own timeout: the record is EXPIRED, nothing is closed and the attempt keeps running.
            op.expire();
            throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                    "session " + attempt.clientName + " could not be connected within the operation's deadline", null,
                    false);
        } catch (ExecutionException e) {
            leaveAttempt(attempt);
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw (RuntimeException) cause;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            leaveAttempt(attempt);
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR, "interrupted while connecting",
                    e, false);
        }
        synchronized (attempt.monitor) {
            attempt.waiters--;
            if (attempt.failure != null) {
                // The task handed the session to its cleanup owner.
                throw attempt.failure;
            }
            if (attempt.claimed || attempt.inCleanup()) {
                // The holders serialise their callers, so this is a guard only.
                throw new JCSMPException("session " + attempt.clientName + " is no longer available to this caller");
            }
            attempt.claimed = true;
        }
    }

    /** A caller gave up. The last one to leave an attempt that connected unclaimed hands it to its cleanup owner. */
    private void leaveAttempt(ConnectAttempt attempt) {
        synchronized (attempt.monitor) {
            attempt.waiters--;
            if (attempt.done && !attempt.claimed && attempt.waiters == 0) {
                attempt.startCleanup();
            }
        }
    }

    /**
     * Holder thread, outside every monitor. A failed or abandoned candidate, or a replaced session, is still being
     * closed: the caller waits for that close under its own budget and then looks again. It never starts another
     * candidate meanwhile, so a holder keeps at most one physical session besides its published one.
     */
    void awaitCleanup(CompletableFuture<Void> cleanup, PublishOperation op, String what) throws SolaceFault {
        try {
            cleanup.get(Math.max(0, op.remaining()), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                    what + " was still being closed when the operation's deadline passed", null, false);
        } catch (ExecutionException e) {
            // The close reported a failure. The cleanup owner logged and counted it, the reservation is released.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "interrupted while " + what + " was being closed", e, false);
        }
    }

    /**
     * The one failure transition of the request reply role, under the lifecycle monitor. A retired handler
     * changes nothing, the published session's handler fails the session, a reserved candidate's handler records
     * the failure on its reservation for the publication to read, any other handler changes nothing.
     */
    void reportReplyDown(PooledSessionEventHandler reporting) {
        synchronized (dedicated) {
            if (reporting.retired) {
                return;
            }
            if (rrSession != null && rrSession.handler == reporting) {
                rrSession.failed = true;
                return;
            }
            if (rrAttempt != null && rrAttempt.handler == reporting) {
                rrAttempt.candidateFailed = true;
            }
        }
    }

    /** The one failure transition of the poll role, the same three cases. */
    void reportPollDown(PooledSessionEventHandler reporting) {
        synchronized (dedicated) {
            if (reporting.retired) {
                return;
            }
            if (pollSession != null && pollSession.handler == reporting) {
                pollSession.failed = true;
                return;
            }
            if (pollAttempt != null && pollAttempt.handler == reporting) {
                pollAttempt.candidateFailed = true;
            }
        }
    }

    /**
     * Under the lifecycle monitor, at the final publication of a dedicated candidate. Null means publish. The
     * record's decision is atomic with its expiry and reads the absolute deadline even when the watchdog is late.
     */
    private SolaceFault publicationRefusal(ConnectAttempt attempt, ConnectAttempt reserved, PublishOperation op,
                                           String what) {
        if (closed) {
            return new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + name + "' closed while " + what + " was being created");
        }
        if (reserved != attempt) {
            return new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    what + " lost its connect reservation");
        }
        if (attempt.candidateFailed) {
            // The candidate went down before it was published: a failed connect.
            return new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    what + " went down before it was published");
        }
        if (!op.publishSession(null, true)) {
            return new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                    what + " connected after the operation's deadline", null, false);
        }
        return null;
    }

    /**
     * Holder thread, outside every other monitor, before a pooled candidate is created. Waits under the record's
     * remaining budget for a free slot below settings.pooledSessionLimit. From here the slot belongs to one physical
     * session. It goes back once: at once when no session was created, otherwise when that session's close
     * finished. Returning or destroying its holder never gives it back.
     */
    void admitPooledSession(PublishOperation op, String connectionId) throws SolaceFault {
        synchronized (pooledSlots) {
            while (pooledSessions >= settings.pooledSessionLimit) {
                if (closed) {
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "Solace connection '" + name + "' is closed");
                }
                long remaining = op.remaining();
                if (op.isExpired() || remaining <= 0) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED, "session " + connectionId
                            + " could not be rebuilt within the operation's deadline: all "
                            + settings.pooledSessionLimit + " pooled sessions of '" + name
                            + "' are connecting, in use or being closed", null, false);
                }
                try {
                    pooledSlots.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "interrupted while session " + connectionId + " waited for a free pooled session", e,
                            false);
                }
            }
            pooledSessions++;
        }
    }

    /** Once per admitted pooled session. Any thread, under any monitor: it takes pooledSlots only. */
    void releasePooledSession() {
        synchronized (pooledSlots) {
            pooledSessions--;
            pooledSlots.notifyAll();
        }
    }

    /** The lifecycle monitor, for the final publication of a pooled session. */
    Object lifecycle() {
        return dedicated;
    }

    /**
     * The poll session, created on first use. The record is the poll operation's record of its timeout and
     * bounds this first connect exactly as sendRequest's phase A bounds the reply session's. The dedicated lock is
     * never held across a broker call: reserve under it, connect outside, publish under it.
     */
    public PollSession pollSession(PublishOperation op) throws JCSMPException, SolaceFault {
        while (true) {
            CompletableFuture<PollSession> creation;
            synchronized (dedicated) {
                if (closed) {
                    throw new JCSMPException("connection closed");
                }
                if (pollSession != null && !pollSession.failed) {
                    return pollSession;
                }
                creation = pollCreation;
                if (creation == null) {
                    pollCreation = new CompletableFuture<>();
                    // The reservation is an attached owner until the attempt ends. The attach is the admission: a
                    // close that sealed the count meanwhile refuses it, nothing to undo.
                    if (!attach(POLL_CREATION)) {
                        pollCreation = null;
                        throw new JCSMPException("connection closed");
                    }
                }
            }
            if (creation != null) {
                // Another caller creates the session: wait for its result under this caller's deadline, then look
                // again. A session that failed meanwhile makes this caller the next creator. A waiter never retires
                // or closes anything.
                try {
                    creation.get(Math.max(0, op.remaining()), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof JCSMPException) {
                        throw (JCSMPException) cause;
                    }
                    if (cause instanceof SolaceFault) {
                        throw (SolaceFault) cause;
                    }
                    throw new JCSMPException(cause.getMessage(), cause);
                } catch (TimeoutException e) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                            "poll timed out while the poll session of '" + name + "' was being created", null, false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                            "interrupted while the poll session of '" + name + "' was being created");
                }
                continue;
            }
            return createPollSession(op);
        }
    }

    /** The creation attempt of the poll session, by the caller that holds the reservation. */
    private PollSession createPollSession(PublishOperation op) throws JCSMPException, SolaceFault {
        CompletableFuture<PollSession> creation;
        synchronized (dedicated) {
            creation = pollCreation;
        }
        PollSession candidate = null;
        ConnectAttempt attempt = null;
        boolean published = false;
        Throwable failure = null;
        try {
            while (true) {
                CompletableFuture<Void> cleanupWait = null;
                synchronized (dedicated) {
                    attempt = pollAttempt;
                    if (attempt != null && attempt.inCleanup()) {
                        cleanupWait = attempt.ended;
                    } else if (attempt == null && pollRetiring != null) {
                        cleanupWait = pollRetiring;
                    } else {
                        if (attempt == null) {
                            // Attaches itself in its constructor. From here the attempt owns the candidate until
                            // publication, and the candidate's retire() is its cleanup owner.
                            candidate = new PollSession(this);
                            // createSession("poll", ...), not connected. On a failure the finally retires it.
                            JCSMPSession created = candidate.createSession();
                            PollSession owned = candidate;
                            // The reservation: one physical attempt of the poll session at a time, held until
                            // its close finished when it is not published.
                            attempt = startAttempt(created, candidate.handler, candidate, dedicated, owned::retire,
                                    cleaned -> {
                                        if (pollAttempt == cleaned) {
                                            pollAttempt = null;
                                        }
                                    });
                            pollAttempt = attempt;
                        } else {
                            // An earlier creator's budget ended: this caller joins its running attempt.
                            candidate = (PollSession) attempt.candidate;
                        }
                        attempt.waiters++;
                    }
                }
                if (cleanupWait == null) {
                    break;
                }
                attempt = null;
                candidate = null;
                awaitCleanup(cleanupWait, op, "the previous poll session of '" + name + "'");
            }
            // Waits under the record's budget and claims the connected session, or faults. A SolaceFault is thrown
            // through.
            awaitAttempt(attempt, op);
            try {
                // failedSupported and rejectedSupported with the outcome negotiation. A TRUE mismatch is reported by the
                // first poll. Outside every monitor.
                candidate.negotiate(attempt.session);
            } catch (JCSMPException e) {
                synchronized (dedicated) {
                    attempt.startCleanup();
                }
                throw e;
            }
            PollSession old = null;
            CompletableFuture<Void> oldCleanup = null;
            SolaceFault refusal;
            // The final publication: the close fence, the reservation, a candidate failure and the record's deadline
            // in one transition under the lifecycle monitor.
            synchronized (dedicated) {
                refusal = publicationRefusal(attempt, pollAttempt, op, "the poll session of '" + name + "'");
                if (refusal != null) {
                    attempt.startCleanup();
                } else {
                    old = pollSession;
                    if (old != null) {
                        // The old handler is retired before the old close is queued and before the publication.
                        old.handler.retire();
                        // The replaced session keeps the role's reservation until its close finished.
                        oldCleanup = new CompletableFuture<>();
                        pollRetiring = oldCleanup;
                    }
                    pollSession = candidate;
                    published = true;
                    pollAttempt = null;
                    attempt.published();
                }
            }
            if (refusal != null) {
                throw refusal;
            }
            if (old != null) {
                CompletableFuture<Void> placeholder = oldCleanup;
                // Outside the monitors.
                old.retire().whenComplete((ignored, error) -> {
                    synchronized (dedicated) {
                        if (pollRetiring == placeholder) {
                            pollRetiring = null;
                        }
                    }
                    placeholder.complete(null);
                });
                counters.sessionsRebuilt.increment();
            }
            return candidate;
        } catch (JCSMPException | SolaceFault | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            // A candidate whose creation failed before its attempt started has no cleanup owner yet.
            if (!published && candidate != null && attempt == null) {
                candidate.retire();
            }
            // A running attempt or a close in progress sends the waiters back to look again.
            boolean retry;
            synchronized (dedicated) {
                retry = pollAttempt != null || pollRetiring != null;
            }
            if (published) {
                creation.complete(candidate);
            } else if (retry) {
                creation.complete(null);
            } else if (failure != null) {
                creation.completeExceptionally(failure);
            }
            synchronized (dedicated) {
                pollCreation = null;
            }
            // The reservation of the creation is released independently of the candidate, exactly once.
            detach(POLL_CREATION);
        }
    }

    /**
     * The request reply session, created on first use. The record bounds the first connect only. The dedicated
     * lock is never held across a broker call: reserve under it, connect outside, publish under it.
     */
    public RequestReplySession requestReplySession(PublishOperation op) throws JCSMPException, SolaceFault {
        while (true) {
            CompletableFuture<RequestReplySession> creation;
            synchronized (dedicated) {
                if (closed) {
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "Solace connection '" + name + "' is closed");
                }
                if (rrSession != null && !rrSession.failed) {
                    return rrSession;
                }
                creation = rrCreation;
                if (creation == null) {
                    rrCreation = new CompletableFuture<>();
                    // The reservation is an attached owner until the attempt ends. The attach is the admission.
                    if (!attach(RR_CREATION)) {
                        rrCreation = null;
                        throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                                "Solace connection '" + name + "' is closed");
                    }
                }
            }
            if (creation != null) {
                // Another caller creates the session: wait for its result under this caller's deadline, then look
                // again. A session that failed meanwhile makes this caller the next creator. A waiter never retires
                // or closes anything.
                try {
                    creation.get(Math.max(0, op.remaining()), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "the request-reply session of '" + name + "' could not be created: " + cause.getMessage(),
                            cause);
                } catch (TimeoutException e) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                            "the operation's deadline passed while the request-reply session of '" + name
                                    + "' was being created", null, false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "interrupted while the request-reply session of '" + name + "' was being created", e,
                            false);
                }
                continue;
            }
            return createRequestReplySession(op);
        }
    }

    /** The creation attempt of the request reply session, by the caller that holds the reservation. */
    private RequestReplySession createRequestReplySession(PublishOperation op) throws JCSMPException, SolaceFault {
        CompletableFuture<RequestReplySession> creation;
        synchronized (dedicated) {
            creation = rrCreation;
        }
        RequestReplySession candidate = null;
        ConnectAttempt attempt = null;
        boolean published = false;
        Throwable failure = null;
        try {
            while (true) {
                CompletableFuture<Void> cleanupWait = null;
                synchronized (dedicated) {
                    attempt = rrAttempt;
                    if (attempt != null && attempt.inCleanup()) {
                        cleanupWait = attempt.ended;
                    } else if (attempt == null && rrRetiring != null) {
                        cleanupWait = rrRetiring;
                    } else {
                        if (attempt == null) {
                            // Attaches itself. Owned by the attempt until publication, and its retire() is the
                            // cleanup owner.
                            candidate = new RequestReplySession(this);
                            // createSession("rr", ...), not connected. On a failure the finally retires it.
                            JCSMPSession created = candidate.createSession();
                            RequestReplySession owned = candidate;
                            // The reservation: one physical attempt of the request reply session at a time,
                            // held until its close finished when it is not published.
                            attempt = startAttempt(created, candidate.handler, candidate, dedicated, owned::retire,
                                    cleaned -> {
                                        if (rrAttempt == cleaned) {
                                            rrAttempt = null;
                                        }
                                    });
                            rrAttempt = attempt;
                        } else {
                            // An earlier creator's budget ended: this caller joins its running attempt.
                            candidate = (RequestReplySession) attempt.candidate;
                        }
                        attempt.waiters++;
                    }
                }
                if (cleanupWait == null) {
                    break;
                }
                attempt = null;
                candidate = null;
                awaitCleanup(cleanupWait, op, "the previous request-reply session of '" + name + "'");
            }
            // Waits under the record's budget and claims the connected session, or faults. A SolaceFault is thrown
            // through.
            awaitAttempt(attempt, op);
            try {
                // Outside every monitor.
                candidate.installProducer();
            } catch (JCSMPException e) {
                // A connected session whose producer failed goes to its cleanup owner, with its reservation.
                synchronized (dedicated) {
                    attempt.startCleanup();
                }
                throw e;
            }
            RequestReplySession old = null;
            CompletableFuture<Void> oldCleanup = null;
            SolaceFault refusal;
            // The final publication: the close fence, the reservation, a candidate failure and the record's deadline
            // in one transition under the lifecycle monitor.
            synchronized (dedicated) {
                refusal = publicationRefusal(attempt, rrAttempt, op, "the request-reply session of '" + name + "'");
                if (refusal != null) {
                    attempt.startCleanup();
                } else {
                    old = rrSession;
                    if (old != null) {
                        // The old handler is retired before the old close is queued and before the publication.
                        old.handler.retire();
                        // The replaced session keeps the role's reservation until its close finished.
                        oldCleanup = new CompletableFuture<>();
                        rrRetiring = oldCleanup;
                    }
                    rrSession = candidate;
                    published = true;
                    rrAttempt = null;
                    attempt.published();
                }
            }
            if (refusal != null) {
                throw refusal;
            }
            if (old != null) {
                CompletableFuture<Void> placeholder = oldCleanup;
                // Outside the monitors.
                old.retire().whenComplete((ignored, error) -> {
                    synchronized (dedicated) {
                        if (rrRetiring == placeholder) {
                            rrRetiring = null;
                        }
                    }
                    placeholder.complete(null);
                });
                counters.sessionsRebuilt.increment();
            }
            return candidate;
        } catch (JCSMPException | SolaceFault | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            // A candidate whose creation failed before its attempt started has no cleanup owner yet.
            if (!published && candidate != null && attempt == null) {
                candidate.retire();
            }
            // A running attempt or a close in progress sends the waiters back to look again.
            boolean retry;
            synchronized (dedicated) {
                retry = rrAttempt != null || rrRetiring != null;
            }
            if (published) {
                creation.complete(candidate);
            } else if (retry) {
                creation.complete(null);
            } else if (failure != null) {
                creation.completeExceptionally(failure);
            }
            synchronized (dedicated) {
                rrCreation = null;
            }
            detach(RR_CREATION);
        }
    }

    /** Submit only, never blocks. An owner that has not yet detached is always accepted. */
    void closeOnCloser(JCSMPSession closing) {
        try {
            closer.execute(() -> {
                Hooks.get().point("closer.beforeClose", closing);
                closeQuietly(closing);
            });
        } catch (RejectedExecutionException e) {
            // Reachable only from expire() running after the last detach.
            log.debug("SolaceConnector [" + name + "] closer already shut down; the session was closed by its owner's own cleanup", e);
        }
    }

    /**
     * Submit only, never blocks. The future completes when the close itself finished on the closer, so a
     * reservation released on it accounts for the session until it is really gone. A close that could not
     * be confirmed is logged and counted, and the future still completes: a holder that kept its reservation for
     * ever would never serve again.
     */
    CompletableFuture<Void> closeTracked(JCSMPSession closing) {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        try {
            closer.execute(() -> {
                try {
                    closeAndConfirm(closing);
                } finally {
                    closed.complete(null);
                }
            });
        } catch (RejectedExecutionException e) {
            // Reachable only after the last detach, when nothing can reserve again.
            log.debug("SolaceConnector [" + name + "] closer already shut down", e);
            closed.complete(null);
        }
        return closed;
    }

    /**
     * Closer thread only. The final close of one physical session, for a candidate, a replaced session and an
     * installed session alike. A close that could not be confirmed is logged and counted once. True when the
     * session is closed.
     */
    boolean closeAndConfirm(JCSMPSession closing) {
        Hooks.get().point("closer.beforeClose", closing);
        closeQuietly(closing);
        if (closing.isClosed()) {
            return true;
        }
        counters.cleanupFailures.increment();
        log.warn("SolaceConnector [" + name + "] a session close could not be confirmed; its reservation is released");
        return false;
    }

    /** Closer thread only. */
    static void closeQuietly(JCSMPSession closing) {
        try {
            if (!closing.isClosed()) {
                closing.closeSession();
            }
        } catch (RuntimeException e) {
            log.debug("SolaceConnector: closing a session failed", e);
        }
    }

    /** The admission: one CAS on the count. False once the count is SEALED, and then nothing was attached. */
    boolean attach(Object owner) {
        for (;;) {
            int count = attached.get();
            if (count == SEALED) {
                return false;
            }
            if (attached.compareAndSet(count, count + 1)) {
                return true;
            }
        }
    }

    /**
     * The last detach after close() seals the count and runs the shutdown block, once. The seal and the decrement
     * are one CAS, so no attach can slip in between.
     */
    void detach(Object owner) {
        for (;;) {
            int count = attached.get();
            // The connection's own attachment is held until close() releases it, so one with closed set is the last.
            boolean last = count == 1 && closed;
            if (!attached.compareAndSet(count, last ? SEALED : count - 1)) {
                continue;
            }
            if (!last) {
                return;
            }
            try {
                // The closer's final task. Every owner's close submission precedes it.
                closer.execute(() -> {
                    try {
                        context.destroy();
                    } catch (RuntimeException e) {
                        log.debug("SolaceConnector [" + name + "] Context destroy failed", e);
                    }
                });
            } catch (RejectedExecutionException e) {
                log.error("SolaceConnector [" + name + "] closer refused the Context destroy", e);
            }
            // Timers only. Every owner that could arm one has detached.
            watchdog.shutdownNow();
            // Never shutdownNow(): a thread inside connect() cannot be interrupted and ends at JCSMP's own timeout,
            // the idle threads die on their own.
            connects.shutdown();
            // Never shutdownNow(): the queued closes run.
            closer.shutdown();
            return;
        }
    }

    /**
     * Any thread. True when no solace library is deployed or the deployed one has a class loader other than the
     * one this class was loaded from, so no caller can reach this connection again. Never throws.
     */
    boolean orphaned() {
        try {
            Map<String, Library> libraries = configuration.getSynapseLibraries();
            Library solace = null;
            for (Map.Entry<String, Library> entry : libraries.entrySet()) {
                if (entry.getKey().endsWith("}solace") || "solace".equals(entry.getKey())) {
                    solace = entry.getValue();
                    break;
                }
            }
            return solace == null || solace.getLibClassLoader() != ownLoader;
        } catch (Exception e) {
            log.debug("SolaceConnector [" + name + "] orphan check failed, treated as not orphaned: " + e);
            return false;
        }
    }

    /** Once. The WARN, then the destroy body of the connector for this class loader. */
    void retireOrphan() {
        if (!retiredOnce.compareAndSet(false, true)) {
            return;
        }
        log.warn("SolaceConnector [" + name + "] connector library replaced; logical connection " + name
                + " closing: open transactions roll back, sessions close");
        SolaceConfigConnector.destroyOnce();
    }

    /** Armed by connect() after the pool exists. An initial delay of 10 s, never 0. */
    public void armExistenceCheck() {
        watchdog.scheduleWithFixedDelay(this::existenceCheck, 10, 10, TimeUnit.SECONDS);
    }

    /** Watchdog thread. An orphan retires itself. remove() submits close() to the closer. */
    private void existenceCheck() {
        if (closed) {
            return;
        }
        if (orphaned()) {
            retireOrphan();
            return;
        }
        if (!ConnectionHandler.getConnectionHandler().checkIfConnectionExists(CONNECTOR_NAME, name)) {
            LogicalConnections.remove(name, this);
        }
    }

    /** SolaceConfigConnector.destroy(), LogicalConnections.create() or the watchdog's existence check. Idempotent. */
    public void close() {
        if (!closedOnce.compareAndSet(false, true)) {
            return;
        }
        PollSession poll;
        RequestReplySession replySession;
        synchronized (dedicated) {
            // Admission stops first: createSession, ownerFor, requestReplySession and new transactions all refuse
            // from here on. Set under the lifecycle monitor every final publication holds, so a candidate is either
            // published before this point, and retired below or by its holder, or never published.
            closed = true;
            poll = pollSession;
            replySession = rrSession;
        }
        // A caller waiting for a pooled session slot sees the close at once.
        synchronized (pooledSlots) {
            pooledSlots.notifyAll();
        }
        // Each retire submits its closes and detaches itself when done.
        if (replySession != null) {
            replySession.retire();
        }
        if (poll != null) {
            poll.retire();
        }
        // The logical connection's own attachment, taken at creation. When the count reaches zero, detach()
        // submits context.destroy(), shuts the watchdog down and then the closer. Until then every owner's close
        // submission is accepted and runs on a live Context, and every owner can still arm a timer.
        detach(this);
        log.info("SolaceConnector [" + name + "] logical connection closing; the Context is destroyed after its last owner detaches");
    }
}
