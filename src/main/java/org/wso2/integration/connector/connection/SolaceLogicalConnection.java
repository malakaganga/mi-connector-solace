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
        return JCSMPFactory.onlyInstance().createSession(sessionProperties, context, handler);
    }

    /**
     * The caller has attached the session to its operation record (3.4) and entered the CONNECTING stage before
     * this call, so an expiry during the connect closes exactly this session. The connect runs on a connect thread
     * and this thread waits for it under the record's remaining budget: JCSMP's connect() does not notice a
     * closeSession() while it waits on the transport or the login reply, so the closer alone could not cut it
     * (A52). A failed connect is handed to the closer, never closed inline. The reconnect guard is registered once
     * the session is connected, and its failure is a connect failure (A45).
     */
    void connectSession(JCSMPSession connecting, PooledSessionEventHandler handler, PublishOperation op)
            throws JCSMPException, SolaceFault {
        // Read before the connect starts: every method of the JCSMP session is synchronized, so a read while
        // connect() holds the monitor would wait for the connect itself (D57).
        String clientName = String.valueOf(connecting.getProperty(JCSMPProperties.CLIENT_NAME));
        Future<?> connect;
        try {
            connect = connects.submit(() -> {
                connecting.connect();
                handler.registerGuard();
                return null;
            });
        } catch (RejectedExecutionException e) {
            // The logical connection is closed.
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + name + "' is closed", e);
        }
        try {
            connect.get(Math.max(0, op.remaining()), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof JCSMPException) {
                closeOnCloser(connecting);
                throw (JCSMPException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw (RuntimeException) cause;
        } catch (TimeoutException e) {
            // The caller's own timeout, the transition of T28 K5: the record is EXPIRED and the session, attached at
            // CONNECTING, goes to the closer. The connect thread finishes on its own and finds its session closed or
            // closing.
            op.expire();
            throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                    "session " + clientName + " could not be connected within the operation's deadline", null, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // The candidate has an owner on every exit (D54).
            closeOnCloser(connecting);
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR, "interrupted while connecting",
                    e, false);
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
        boolean published = false;
        Throwable failure = null;
        try {
            // Attaches itself in its constructor. From here the attempt owns the candidate until publication.
            candidate = new PollSession(this);
            // createSession("poll", ...), the attach to the record, connectSession and the transfer decision. On a
            // failure the session is with the closer and the field is null.
            candidate.connect(op);
            PollSession old = null;
            synchronized (dedicated) {
                // The publication fence: a logical connection closed meanwhile never gets a current session.
                if (!closed) {
                    old = pollSession;
                    pollSession = candidate;
                    published = true;
                }
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
            // Every unsuccessful exit: the candidate's session, if any, goes to the closer and the candidate's
            // attachment is released. retire() is idempotent and tolerates a null session.
            if (!published && candidate != null) {
                candidate.retire();
            }
            // The waiters learn the result before the reservation ends (A53).
            if (published) {
                creation.complete(candidate);
            } else if (failure != null) {
                creation.completeExceptionally(failure);
            }
            synchronized (dedicated) {
                pollCreation = null;
            }
            // The reservation is released independently of the candidate, exactly once.
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
        boolean published = false;
        Throwable failure = null;
        try {
            // Attaches itself. Owned by the attempt until publication.
            candidate = new RequestReplySession(this);
            JCSMPSession created = candidate.createSession();
            if (op != null && !op.attachSession(created)) {
                // Refused: the finally retires the candidate and its session goes to the closer unused.
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "operation expired before the reply session connected", null, false);
            }
            // The stage tells the watchdog the created session is in use: from here an expiry closes it (A51).
            if (op != null && !op.enterStage(PublishOperation.Stage.CONNECTING)) {
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "operation expired before the reply session connected", null, false);
            }
            // An expiry during the connect closes exactly the created session and the connect throws.
            candidate.connectSession(op);
            if (op != null && !op.transferSession()) {
                // The transfer decision, atomic with expire() and with the deadline (3.4). Refused means the
                // session is already with the closer, so the field is cleared and retire() does not submit it twice.
                candidate.session = null;
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "operation expired as the reply session connected", null, true);
            }
            // The transfer won: the record holds nothing, and the stage returns to BEFORE_SEND (A51).
            if (op != null) {
                op.enterStage(PublishOperation.Stage.BEFORE_SEND);
            }
            RequestReplySession old = null;
            synchronized (dedicated) {
                // The logical close fence: a logical connection closed meanwhile never gets a current session.
                if (!closed) {
                    old = rrSession;
                    rrSession = candidate;
                    published = true;
                }
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
            // Every unsuccessful exit, the SolaceFault exits included.
            if (!published && candidate != null) {
                candidate.retire();
            }
            // The waiters learn the result before the reservation ends (A53).
            if (published) {
                creation.complete(candidate);
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
