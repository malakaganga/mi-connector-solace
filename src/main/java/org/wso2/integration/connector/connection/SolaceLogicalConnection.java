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

import static org.wso2.integration.connector.constants.SolaceConstants.CONNECTOR_NAME;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;

/**
 * One per connection name: the JCSMP Context, the watchdog, the closer, the dedicated sessions, the counters
 * and the client name counter (guide 3.3, design 6.4). Created by SolaceConfigConnector.connect() through
 * LogicalConnections.create(), closed when the connection is undeployed or redeployed.
 */
public final class SolaceLogicalConnection {

    private static final Log log = LogFactory.getLog(SolaceLogicalConnection.class);

    public final String name;
    final String nodeId;
    // host, vpn, credentials, TLS, retries, window. CLIENT_NAME is set per session.
    final JCSMPProperties baseProperties;
    public final ConnectionSettings settings;
    // One Context per logical connection (D19).
    final Context context;
    // Timers only, never a JCSMP call.
    final ScheduledThreadPoolExecutor watchdog;
    // The only connector thread that blocks on closeSession().
    final ThreadPoolExecutor closer;
    // The connect threads. A connect runs here under the operation's deadline, because JCSMP's connect() does not
    // notice a closeSession() while it waits on the transport or the login reply (A52).
    final ExecutorService connects;
    // The connect retries of the template, JCSMP's default 0 when absent. A physical attempt connects once, so the
    // retries bound the attempts of the recovery loop instead (A62).
    final int connectRetries;
    private final AtomicBoolean retriesLogged = new AtomicBoolean();
    public final ConnectionCounters counters = new ConnectionCounters();
    // 1, 2, ... assigned to pooled SolaceConnection objects at creation.
    final AtomicInteger pooledIndex = new AtomicInteger();
    // 1, 2, ... the n of each poll owner thread name (3.5).
    final AtomicInteger pollOwnerIndex = new AtomicInteger();
    final OAuthTokenSource tokenSource;
    // Guards the dedicated session fields below.
    private final Object dedicated = new Object();
    // 3.5, lazy.
    private PollSession pollSession;
    private RequestReplySession rrSession;
    // The creation attempt in progress, null when none (guarded by dedicated). A caller that finds one waits on it
    // under its own deadline instead of being refused (A53).
    private CompletableFuture<PollSession> pollCreation;
    private CompletableFuture<RequestReplySession> rrCreation;
    // The physical connect attempt in progress of each dedicated session, null when none (guarded by dedicated).
    // It outlives a creator whose budget ended, and the next creator joins it (A62).
    private ConnectAttempt rrAttempt;
    private ConnectAttempt pollAttempt;
    // Guarded by dedicated. PollSession's generation number.
    int pollGeneration;
    // Owner tokens of the two creation reservations (the attachment table).
    static final Object POLL_CREATION = new Object();
    static final Object RR_CREATION = new Object();
    // The attached owner count. SEALED once the last owner detached after close().
    private final AtomicInteger attached = new AtomicInteger();
    private static final int SEALED = Integer.MIN_VALUE;
    private final AtomicBoolean closedOnce = new AtomicBoolean();
    public volatile boolean closed = false;
    // A48: the library this class was loaded from, and the configuration that names the deployed one.
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
        // The static token of the template, if any, is the fallback of 2.3.
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
        // A physical attempt connects once. The configured retries bound the recovery loop instead (A62).
        channelProperties.setConnectRetries(0);
        channelProperties.setConnectRetriesPerHost(0);
        if (retriesLogged.compareAndSet(false, true)) {
            log.info("SolaceConnector [" + name + "] connect retries " + connectRetries
                    + " apply to the recovery loop, each physical attempt connects once");
        }
        return JCSMPFactory.onlyInstance().createSession(sessionProperties, context, handler);
    }

    /**
     * One physical connect attempt of a holder, the reservation of A62. A holder keeps at most one at a time under
     * its publication monitor, taken before the connect is submitted and released when the attempt ended: the
     * session published, or handed to the closer after a failed connect or after every caller gave up.
     */
    static final class ConnectAttempt {

        final JCSMPSession session;
        final PooledSessionEventHandler handler;
        // Read before the connect starts: every method of the JCSMP session is synchronized, so a read while
        // connect() holds the monitor would wait for the connect itself (D57).
        final String clientName;
        // The candidate of a dedicated session, the RequestReplySession or the PollSession built for this attempt.
        final Object candidate;
        // The holder's publication monitor.
        final Object monitor;
        // The holder's release: it clears the reservation and hands the session to the closer. Runs under monitor.
        private final Runnable release;
        Future<?> connect;
        // The callers waiting on the connect. Guarded by monitor.
        int waiters;
        // The connect ended. Guarded by monitor.
        boolean done;
        // A caller took the connected session for its publication. Guarded by monitor.
        boolean claimed;
        private boolean released;
        // The connect failure the waiters rethrow. Guarded by monitor.
        JCSMPException failure;

        ConnectAttempt(JCSMPSession session, PooledSessionEventHandler handler, Object candidate, Object monitor,
                       Runnable release) {
            this.session = session;
            this.handler = handler;
            this.clientName = String.valueOf(session.getProperty(JCSMPProperties.CLIENT_NAME));
            this.candidate = candidate;
            this.monitor = monitor;
            this.release = release;
        }

        /** Under monitor. */
        void releaseOnce() {
            if (released) {
                return;
            }
            released = true;
            release.run();
        }
    }

    /**
     * Holder thread, under the holder's publication monitor with no attempt reserved. Submits the connect of the
     * candidate and returns the attempt the holder stores as its reservation (A62). The connect thread connects
     * once, createSession set the JCSMP retries to 0, and registers the reconnect guard (A45). When the connect
     * fails, or succeeds after every caller gave up, the task itself hands the session to the closer and releases
     * the reservation, so the closer never waits inside a running connect.
     */
    ConnectAttempt startAttempt(JCSMPSession candidate, PooledSessionEventHandler handler, Object dedicatedCandidate,
                                Object monitor, Runnable release) throws SolaceFault {
        ConnectAttempt attempt = new ConnectAttempt(candidate, handler, dedicatedCandidate, monitor, release);
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
                // Failed, or connected with no caller left to publish it: the session goes to the closer after the
                // connect ended, never during it.
                if (!connected || (attempt.waiters == 0 && !attempt.claimed)) {
                    attempt.releaseOnce();
                }
            }
        }
    }

    /**
     * Holder thread, outside the monitor, counted as a waiter of the attempt. Waits for the connect under the
     * record's remaining budget and claims the connected session for publication through the transfer rule of
     * 3.4. A caller whose budget ends records EXPIRED and leaves the attempt running for the next caller. A failed
     * connect is thrown to the caller as its own failure (A62). On return the caller publishes the session.
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
                // The task handed the session to the closer and released the holder.
                throw attempt.failure;
            }
            if (attempt.claimed) {
                // Another caller publishes it. The holders serialise their callers, so this is a guard only.
                throw new JCSMPException("session " + attempt.clientName + " is being published by another caller");
            }
            if (!op.transferSession()) {
                // The transfer rule of 3.4: a late connect is never published. With no caller left the session goes
                // to the closer.
                if (attempt.waiters == 0) {
                    attempt.releaseOnce();
                }
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "session " + attempt.clientName + " connected after the operation's deadline", null, false);
            }
            attempt.claimed = true;
        }
    }

    /** A caller gave up. The last one to leave an attempt that connected unclaimed hands the session to the closer. */
    private void leaveAttempt(ConnectAttempt attempt) {
        synchronized (attempt.monitor) {
            attempt.waiters--;
            if (attempt.done && !attempt.claimed && attempt.waiters == 0) {
                attempt.releaseOnce();
            }
        }
    }

    /** The handler of the published request reply session only writes its failed flag (A63). */
    boolean markReplyFailed(RequestReplySession reporting) {
        synchronized (dedicated) {
            if (rrSession != reporting) {
                return false;
            }
            reporting.failed = true;
            return true;
        }
    }

    /** The handler of the published poll session only writes its failed flag (A63). */
    boolean markPollFailed(PollSession reporting) {
        synchronized (dedicated) {
            if (pollSession != reporting) {
                return false;
            }
            reporting.failed = true;
            return true;
        }
    }

    /**
     * The poll session, created on first use. The record is the poll operation's record of its timeout (3.5) and
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
                // or closes anything (A53).
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
            synchronized (dedicated) {
                attempt = pollAttempt;
                if (attempt == null) {
                    // Attaches itself in its constructor. From here the attempt owns the candidate until publication.
                    candidate = new PollSession(this);
                    // createSession("poll", ...), not connected. On a failure the finally retires the candidate.
                    JCSMPSession created = candidate.createSession();
                    PollSession reserved = candidate;
                    // The reservation: one physical attempt of the poll session at a time (A62).
                    attempt = startAttempt(created, candidate.handler, candidate, dedicated, () -> {
                        pollAttempt = null;
                        reserved.retire();
                    });
                    pollAttempt = attempt;
                } else {
                    // An earlier creator's budget ended: this caller joins its running attempt (A62).
                    candidate = (PollSession) attempt.candidate;
                }
                attempt.waiters++;
            }
            // Waits under the record's budget and claims the connected session, or faults. A SolaceFault is thrown
            // through.
            awaitAttempt(attempt, op);
            try {
                // failedSupported and rejectedSupported with the 1.6 algorithm. A TRUE mismatch is reported by the
                // first poll.
                candidate.negotiate(attempt.session);
            } catch (JCSMPException e) {
                synchronized (dedicated) {
                    attempt.releaseOnce();
                }
                throw e;
            }
            PollSession old = null;
            synchronized (dedicated) {
                if (candidate.handler.failedBeforePublication) {
                    // The candidate went down before it was published: a failed connect (A63).
                    attempt.releaseOnce();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "the poll session of '" + name + "' went down before it was published");
                }
                // The publication fence: a logical connection closed meanwhile never gets a current session.
                if (!closed) {
                    old = pollSession;
                    if (old != null) {
                        // The old handler is retired before the old close is queued and before the publication (A63).
                        old.handler.retire();
                    }
                    pollSession = candidate;
                    published = true;
                }
                // The reservation ends here, published or not.
                pollAttempt = null;
            }
            if (!published) {
                throw new JCSMPException("logical connection '" + name + "' closed during the poll session creation");
            }
            if (old != null) {
                old.retire();
                counters.sessionsRebuilt.increment();
            }
            return candidate;
        } catch (JCSMPException | SolaceFault | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            // An attempt that still runs stays reserved for the next caller (A62).
            boolean reserved;
            synchronized (dedicated) {
                reserved = attempt != null && pollAttempt == attempt && !attempt.done;
            }
            // Every other unsuccessful exit: the candidate's session, if any, goes to the closer and the candidate's
            // attachment is released. retire() is idempotent and tolerates a null session.
            if (!published && candidate != null && !reserved) {
                candidate.retire();
            }
            // The waiters learn the result before the reservation ends (A53). A running attempt sends them back to
            // the healthy check, so the next one joins it.
            if (published) {
                creation.complete(candidate);
            } else if (reserved) {
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
                // or closes anything (A53).
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
            synchronized (dedicated) {
                attempt = rrAttempt;
                if (attempt == null) {
                    // Attaches itself. Owned by the attempt until publication.
                    candidate = new RequestReplySession(this);
                    // createSession("rr", ...), not connected. On a failure the finally retires the candidate.
                    JCSMPSession created = candidate.createSession();
                    RequestReplySession reserved = candidate;
                    // The reservation: one physical attempt of the request reply session at a time (A62).
                    attempt = startAttempt(created, candidate.handler, candidate, dedicated, () -> {
                        rrAttempt = null;
                        reserved.retire();
                    });
                    rrAttempt = attempt;
                } else {
                    // An earlier creator's budget ended: this caller joins its running attempt (A62).
                    candidate = (RequestReplySession) attempt.candidate;
                }
                attempt.waiters++;
            }
            // Waits under the record's budget and claims the connected session, or faults. A SolaceFault is thrown
            // through.
            awaitAttempt(attempt, op);
            try {
                candidate.installProducer();
            } catch (JCSMPException e) {
                // A connected session whose producer failed goes to the closer, with its reservation.
                synchronized (dedicated) {
                    attempt.releaseOnce();
                }
                throw e;
            }
            RequestReplySession old = null;
            synchronized (dedicated) {
                if (candidate.handler.failedBeforePublication) {
                    // The candidate went down before it was published: a failed connect (A63).
                    attempt.releaseOnce();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "the request-reply session of '" + name + "' went down before it was published");
                }
                // The logical close fence: a logical connection closed meanwhile never gets a current session.
                if (!closed) {
                    old = rrSession;
                    if (old != null) {
                        // The old handler is retired before the old close is queued and before the publication (A63).
                        old.handler.retire();
                    }
                    rrSession = candidate;
                    published = true;
                }
                // The reservation ends here, published or not.
                rrAttempt = null;
            }
            if (!published) {
                throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                        "Solace connection '" + name + "' closed during the reply session creation");
            }
            if (old != null) {
                old.retire();
                counters.sessionsRebuilt.increment();
            }
            return candidate;
        } catch (JCSMPException | SolaceFault | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            // An attempt that still runs stays reserved for the next caller (A62).
            boolean reserved;
            synchronized (dedicated) {
                reserved = attempt != null && rrAttempt == attempt && !attempt.done;
            }
            // Every other unsuccessful exit, the SolaceFault exits included.
            if (!published && candidate != null && !reserved) {
                candidate.retire();
            }
            // The waiters learn the result before the reservation ends (A53). A running attempt sends them back to
            // the healthy check, so the next one joins it.
            if (published) {
                creation.complete(candidate);
            } else if (reserved) {
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
            // Reachable only from expire() running after the last detach (3.4).
            log.debug("SolaceConnector [" + name + "] closer already shut down; the session was closed by its owner's own cleanup", e);
        }
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
            // the idle threads die on their own (A52).
            connects.shutdown();
            // Never shutdownNow(): the queued closes run.
            closer.shutdown();
            return;
        }
    }

    /**
     * Any thread. True when no solace library is deployed or the deployed one has a class loader other than the
     * one this class was loaded from, so no caller can reach this connection again (A48). Never throws.
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

    /** Once. The WARN, then the destroy body of the connector for this class loader (A48). */
    void retireOrphan() {
        if (!retiredOnce.compareAndSet(false, true)) {
            return;
        }
        log.warn("SolaceConnector [" + name + "] connector library replaced; logical connection " + name
                + " closing: open transactions roll back, sessions close");
        SolaceConfigConnector.destroyOnce();
    }

    /** Armed by connect() after the pool exists (3.8). An initial delay of 10 s, never 0. */
    public void armExistenceCheck() {
        watchdog.scheduleWithFixedDelay(this::existenceCheck, 10, 10, TimeUnit.SECONDS);
    }

    /** Watchdog thread. An orphan retires itself (A48). remove() submits close() to the closer (G12). */
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
        // Admission stops first: createSession, ownerFor, requestReplySession and new transactions all refuse from
        // here on.
        closed = true;
        PollSession poll;
        RequestReplySession replySession;
        synchronized (dedicated) {
            poll = pollSession;
            replySession = rrSession;
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
