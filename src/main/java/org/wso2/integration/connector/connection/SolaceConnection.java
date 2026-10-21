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

import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CacheLiveDataAction;
import com.solacesystems.jcsmp.CacheRequestResult;
import com.solacesystems.jcsmp.CacheSession;
import com.solacesystems.jcsmp.CacheSessionProperties;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.Connection;
import org.wso2.integration.connector.core.connection.ConnectionConfig;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.Backoff;

import static org.wso2.integration.connector.constants.SolaceConstants.CONNECTOR_NAME;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_ACTIVE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_ACTIVE;

/**
 * Manages a connection to the Solace event broker.
 * Wraps a JCSMPSession and XMLMessageProducer.
 */
public class SolaceConnection implements Connection {

    private static final Log log = LogFactory.getLog(SolaceConnection.class);

    final SolaceLogicalConnection lc;
    // From lc.pooledIndex, fixed for the object's life. The rebuilt session reuses the name with this index.
    final int index;
    // The log identity, name and index.
    final String connectionId;
    public volatile JCSMPSession session;
    volatile XMLMessageProducer producer;
    volatile int generation = 0;
    public volatile boolean failed = false;
    // The handler of the current session, with the reconnect guard registered at connect (A45).
    private volatile PooledSessionEventHandler handler;
    // The publication monitor: the session, the producer and the handler change under it, the handler of the
    // current session alone writes failed under it, and the connect reservation lives under it (A62, A63).
    private final Object publication = new Object();
    // The physical connect attempt in progress of this holder, null when none (guarded by publication). It outlives
    // a borrower whose budget ended, and the next borrower joins it (A62).
    private SolaceLogicalConnection.ConnectAttempt connectAttempt;
    // 3.4: the producer lock, the pending acknowledgements and the send algorithm. Rebuilt with the session.
    public final SessionPublisher publisher;
    // The one cleanup claim (orphan()).
    private final AtomicBoolean orphaned = new AtomicBoolean();
    private XMLMessageConsumer consumer;
    private final Object consumerLock = new Object();
    private TransactedSession txSession;
    private XMLMessageProducer txProducer;
    /**
     * Transacted FlowReceivers, one per queue name, lazily created the first time the
     * caller polls a given queue inside the current transaction. The selector each
     * receiver was created with is recorded so subsequent polls of the same queue can
     * be rejected if they request a different filter — JCSMP binds selectors at flow
     * creation, so a mismatch on reuse would silently apply the wrong filter.
     * Lifecycle is bound to {@link #txSession}: all receivers are closed in
     * {@link #closeTransactedSessionInternal} on commit/rollback.
     */
    private final Map<String, TxFlowEntry> txFlowReceivers = new ConcurrentHashMap<>();
    private final Object txLock = new Object();

    /** The pooled holder, unconnected. The first ensureSession of a borrowing operation connects it. */
    public SolaceConnection(SolaceLogicalConnection lc, int index) {
        this.lc = lc;
        this.index = index;
        this.connectionId = lc.name + "-" + index;
        this.publisher = new SessionPublisher(lc, connectionId);
    }

    /**
     * Gets the underlying JCSMP session.
     *
     * @return the session
     */
    public JCSMPSession getSession() {
        return session;
    }

    /**
     * Gets the connection identifier.
     *
     * @return the connection ID
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Checks if the connection is active.
     *
     * @return true if the session is not closed
     */
    public boolean isConnected() {
        return session != null && !session.isClosed() && !failed;
    }

    /**
     * Design 7.10 ensureSession(), the first call of every operation on a pooled connection after the borrow.
     * The operation's own record stays attached: the send that follows refuses an expired record and the
     * operation's finally discards the session (3.4).
     */
    public void ensureSession(PublishOperation op) throws SolaceFault {
        ensureSession(op, op.deadline, false);
    }

    /**
     * For callers without a record of their own. The attempt record keeps the caller's absolute deadline, so the
     * watchdog closes the attached candidate at the deadline exactly as on the publish path. The record is
     * finished before the caller touches the session, so it never governs the session afterwards.
     */
    public void ensureSession(long deadlineMillis) throws SolaceFault {
        PublishOperation attempt = PublishOperation.registerUntil(lc, deadlineMillis);
        try {
            // true: the record hands the session over at publication through the atomic transfer decision.
            ensureSession(attempt, deadlineMillis, true);
        } finally {
            attempt.finish();
        }
    }

    private void ensureSession(PublishOperation op, long deadlineMillis, boolean transferAtPublication)
            throws SolaceFault {
        // Before the healthy return: a closed logical connection serves nothing.
        if (lc.closed) {
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + lc.name + "' is closed");
        }
        // Before the healthy return too: a spent budget never returns a session. Nothing was sent.
        if (op.isExpired() || System.currentTimeMillis() >= deadlineMillis) {
            throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                    "the operation's deadline passed before the session was checked", null, false);
        }
        JCSMPSession current = session;
        if (current != null && !current.isClosed() && !failed) {
            return;
        }
        if (hasActiveTransaction()) {
            throw new SolaceFault(FAULT_TRANSACTION_ACTIVE, ERROR_CODE_TRANSACTION_ACTIVE,
                    "connection " + connectionId + " owns an active transaction and its session cannot be rebuilt");
        }
        int attempt = 1;
        while (true) {
            // Terminal: a closed logical connection never reopens. Tested before every attempt.
            if (lc.closed) {
                throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                        "Solace connection '" + lc.name + "' is closed");
            }
            // An already spent budget never starts an attempt. Nothing was sent.
            if (op.isExpired() || System.currentTimeMillis() >= deadlineMillis) {
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "session " + connectionId + " could not be rebuilt within the operation's deadline", null, false);
            }
            try {
                SolaceLogicalConnection.ConnectAttempt running;
                synchronized (publication) {
                    running = connectAttempt;
                    if (running == null) {
                        // Not connected yet. The same client name as the predecessor, so the broker evicts the
                        // zombie.
                        PooledSessionEventHandler candidateHandler = new PooledSessionEventHandler(this);
                        JCSMPSession candidate = lc.createSession(String.valueOf(index), candidateHandler);
                        candidateHandler.bind(candidate);
                        // The reservation: one physical attempt of this holder at a time (A62).
                        running = lc.startAttempt(candidate, candidateHandler, null, publication, () -> {
                            lc.closeOnCloser(candidate);
                            connectAttempt = null;
                        });
                        connectAttempt = running;
                    }
                    running.waiters++;
                }
                // Waits under the record's remaining budget and claims the connected session, or faults: a spent
                // budget leaves the attempt running for the next borrower (A62). A SolaceFault is thrown through,
                // nothing was sent.
                lc.awaitAttempt(running, op);
                JCSMPSession candidate = running.session;
                PooledSessionEventHandler candidateHandler = running.handler;
                XMLMessageProducer candidateProducer;
                try {
                    candidateProducer = candidate.getMessageProducer(publisher.eventHandler());
                } catch (JCSMPException e) {
                    // The creation obligation lasts through producer creation.
                    synchronized (publication) {
                        running.releaseOnce();
                    }
                    throw e;
                }
                synchronized (publication) {
                    if (candidateHandler.failedBeforePublication) {
                        // The candidate went down before it was published: a failed connect (A63).
                        running.releaseOnce();
                        throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                                "session " + connectionId + " went down before it was published");
                    }
                    JCSMPSession old = session;
                    PooledSessionEventHandler oldHandler = handler;
                    // The old handler is retired before the old close is queued and before the publication (A63).
                    if (oldHandler != null) {
                        oldHandler.retire();
                    }
                    // Detached, never closed here: the old session's close closes its consumer. The next
                    // ensureConsumer builds the new session's consumer from the new guard (A64).
                    synchronized (consumerLock) {
                        consumer = null;
                    }
                    session = candidate;
                    producer = candidateProducer;
                    handler = candidateHandler;
                    failed = false;
                    generation++;
                    publisher.swap(candidate, candidateProducer);
                    // The old producer dies with its session.
                    if (old != null) {
                        lc.closeOnCloser(old);
                    }
                    // The reservation ends with the publication.
                    connectAttempt = null;
                }
                lc.counters.sessionsRebuilt.increment();
                if (generation == 1) {
                    log.info("SolaceConnector [" + lc.name + "] session " + connectionId + " connected (generation 1)");
                } else {
                    log.warn("SolaceConnector [" + lc.name + "] session " + connectionId + " rebuilt (generation " + generation + ")");
                }
                if (!transferAtPublication && !op.attachSession(candidate)) {
                    // The publish path keeps the record on the session it publishes with. Refused once the record
                    // expired meanwhile: the session stays healthy at BEFORE_SEND (A51).
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                            "session " + connectionId + " connected after the operation's deadline", null, false);
                }
                // Back to BEFORE_SEND once the session is taken over, so an expiry in the waits that follow leaves
                // it alone (A51).
                if (!op.enterStage(PublishOperation.Stage.BEFORE_SEND)) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                            "session " + connectionId + " connected after the operation's deadline", null, false);
                }
                return;
            } catch (JCSMPException e) {
                // The failure was the close itself: no back off.
                if (lc.closed) {
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "Solace connection '" + lc.name + "' is closed", e);
                }
                long delay = Backoff.delay(lc.settings.recoveryInitialDelayMillis, lc.settings.recoveryProgression,
                        lc.settings.recoveryMaxDelayMillis, attempt);
                boolean budget = lc.settings.recoveryMaxAttempts >= 0 && attempt >= lc.settings.recoveryMaxAttempts;
                // The configured connect retries bound the physical attempts of this loop, each connecting once. The
                // value -1 is unlimited with the back off (A62).
                boolean retriesSpent = lc.connectRetries >= 0 && attempt > lc.connectRetries;
                boolean deadline = op.isExpired() || System.currentTimeMillis() + delay >= deadlineMillis;
                if (budget || retriesSpent || deadline) {
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "session " + connectionId + " could not be rebuilt: " + e.getMessage(), e);
                }
                log.warn("SolaceConnector [" + lc.name + "] session " + connectionId + " rebuild attempt " + attempt
                        + " failed; next in " + delay + " ms: " + e.getMessage());
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                            "interrupted while rebuilding session " + connectionId, ie, false);
                }
                attempt++;
            }
        }
    }

    /** The handler of the current session alone writes the failed flag, under the publication monitor (A63). */
    boolean markFailed(PooledSessionEventHandler reporting) {
        synchronized (publication) {
            if (handler != reporting) {
                return false;
            }
            failed = true;
            return true;
        }
    }

    /**
     * The one way a borrowed holder goes back, used by every operation's finally and by the transaction
     * completion. The core returns by name only, so the return is coordinated with replacement through the per
     * name lock and with the core's own removal through the idempotent orphan path. No broker call happens under
     * the lock: disconnect() only submits, and the post return orphan runs outside it.
     */
    public void returnToPool() {
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        boolean orphan;
        synchronized (LogicalConnections.lockFor(lc.name)) {
            // Replaced, closed, or the pool already removed by the core: never enter another pool by name.
            orphan = LogicalConnections.get(lc.name) != lc || lc.closed
                    || !handler.checkIfConnectionExists(CONNECTOR_NAME, lc.name);
            if (!orphan) {
                // The original pool, by construction of the check above. A pool closed meanwhile destroys the
                // holder inside this call, through destroyObject and orphan().
                handler.returnConnection(CONNECTOR_NAME, lc.name, this);
                // The core removed the pool between the check and the return. Its removal does not take our lock.
                orphan = !handler.checkIfConnectionExists(CONNECTOR_NAME, lc.name);
            }
        }
        if (orphan) {
            orphan();
        }
    }

    /**
     * The idempotent claim is around the whole cleanup: whoever wins it submits the closes and then detaches,
     * exactly once. The loser returns at once.
     */
    public void orphan() {
        if (!orphaned.compareAndSet(false, true)) {
            return;
        }
        try {
            disconnect();
        } finally {
            lc.detach(this);
        }
    }

    public TransactedSession getOrCreateTransactedSession() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.debug("Creating new transacted session on connection: " + connectionId);
                // Build into a local first so a producer-create failure can't leave the
                // connection with txSession set but txProducer null — that state would
                // poison the connection for every subsequent borrower from the pool.
                TransactedSession newSession = session.createTransactedSession();
                try {
                    ProducerFlowProperties producerProps = new ProducerFlowProperties();
                    txProducer = newSession.createProducer(producerProps, publisher.eventHandler());
                    txSession = newSession;
                    log.debug("Transacted session created on connection: " + connectionId);
                } catch (JCSMPException e) {
                    log.error("Failed to create transacted producer on connection: " + connectionId
                            + " — closing transacted session: " + e.getMessage(), e);
                    try {
                        // Close the half-built transacted session so it isn't leaked; the
                        // original producer-create failure is rethrown below.
                        newSession.close();
                    } catch (Exception ignored) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error closing half-built transacted session during cleanup on"
                                    + " connection: " + connectionId + " — " + ignored.getMessage());
                        }
                    }
                    throw e;
                }
            } else {
                log.info("Reusing existing transacted session on connection: " + connectionId);
            }
            return txSession;
        }
    }

    public void commitTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("commitTransaction called but no active transacted session on connection: "
                        + connectionId);
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            log.debug("Committing transacted session on connection: " + connectionId);
            try {
                txSession.commit();
                log.info("Transaction committed on connection: " + connectionId);
            } catch (JCSMPException e) {
                log.error("Transacted session commit failed on connection: " + connectionId
                        + ": " + e.getMessage(), e);
                throw e;
            } finally {
                closeTransactedSessionInternal();
                log.debug("Transacted session closed on connection: " + connectionId + " (post-commit)");
            }
        }
    }

    public void rollbackTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("rollbackTransaction called but no active transacted session on connection: "
                        + connectionId);
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            log.debug("Rolling back transacted session on connection: " + connectionId);
            try {
                txSession.rollback();
                log.warn("Transaction rolled back on connection: " + connectionId);
            } catch (JCSMPException e) {
                log.error("Transacted session rollback failed on connection: " + connectionId
                        + ": " + e.getMessage(), e);
                throw e;
            } finally {
                closeTransactedSessionInternal();
                log.debug("Transacted session closed on connection: " + connectionId + " (post-rollback)");
            }
        }
    }

    public boolean hasActiveTransaction() {
        synchronized (txLock) {
            return txSession != null;
        }
    }

    private void closeTransactedSessionInternal() {
        // Close any transacted FlowReceivers first — they live on the txSession and would
        // otherwise be orphaned by closing the session out from under them.
        if (!txFlowReceivers.isEmpty()) {
            for (Map.Entry<String, TxFlowEntry> entry : txFlowReceivers.entrySet()) {
                try {
                    entry.getValue().receiver.close();
                } catch (Exception e) {
                    log.warn("Error closing transacted flow receiver for queue '"
                            + entry.getKey() + "' on connection " + connectionId + ": "
                            + e.getMessage());
                }
            }
            txFlowReceivers.clear();
        }
        if (txProducer != null) {
            txProducer.close();
            txProducer = null;
        }
        if (txSession != null) {
            txSession.close();
            txSession = null;
        }
    }

    public void closeTransactedSession() {
        synchronized (txLock) {
            closeTransactedSessionInternal();
        }
    }

    public PublishResult publishTransacted(String destinationType, String destinationName,
                                       String payload, String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, String httpContentType,
                                       MessageContext synCtx)
        throws JCSMPException {
        synchronized (txLock) {
            if (txProducer == null) {
                log.error("publishTransacted called but no transacted session active on connection: "
                        + connectionId);
                throw new JCSMPException(
                    "publishTransacted called but no transacted session active on connection: "
                    + connectionId);
            }
            Destination destination = MessageBuilder.resolveDestination(destinationType, destinationName);
            BytesXMLMessage msg = MessageBuilder.createMessage(messageType, payload, synCtx);
            if (httpContentType != null && !httpContentType.isEmpty()) {
                msg.setHTTPContentType(httpContentType);
            }
            msg.setDeliveryMode(MessageBuilder.resolveDeliveryMode(deliveryMode));
            MessageBuilder.applyMessageProperties(msg, properties);
            // Stable correlation key per transacted publish — surfaced in the PublishResult and
            // visible in broker-side message-spool browsing for "where did this message go" diagnostics.
            String correlationKey = "tx-" + connectionId + "-" + UUID.randomUUID();
            msg.setCorrelationKey(correlationKey);
            log.debug("publishTransacted: sending to " + destinationType + " '" + destinationName
                    + "' on connectionId=" + connectionId + " (correlationKey=" + correlationKey + ")");
            try {
                txProducer.send(msg, destination);
            } catch (JCSMPException e) {
                log.error("publishTransacted: send failed on connectionId=" + connectionId
                        + " (correlationKey=" + correlationKey + "): " + e.getMessage(), e);
                throw e;
            }
            log.debug("publishTransacted: send returned (pending commit) on connectionId=" + connectionId
                    + " (correlationKey=" + correlationKey + ")");
            return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey, null);
        }
    }

    /** Transaction executor thread only (3.7). The reply goes out on the transacted producer and is visible at the commit. */
    public void sendReplyTransacted(BytesXMLMessage request, BytesXMLMessage reply) throws JCSMPException {
        synchronized (txLock) {
            if (txProducer == null) {
                throw new JCSMPException(
                    "sendReplyTransacted called but no transacted session active on connection: "
                    + connectionId);
            }
            txProducer.sendReply(request, reply);
        }
    }

    /**
     * Synchronously polls one message from a queue inside the active transaction. The
     * message is held by the broker until the surrounding transaction commits (then acked
     * and removed) or rolls back (then redelivered) — the caller does not (and cannot)
     * settle it manually.
     *
     * <p>Provisions a transacted FlowReceiver on the txSession the first time a given
     * queue is polled in this transaction; subsequent polls of the same queue reuse the
     * same receiver. All receivers are closed by {@link #closeTransactedSessionInternal}
     * on commit/rollback.</p>
     *
     * @param queueName     the durable queue to poll
     * @param timeoutMillis maximum time to block waiting for a message
     * @param selector      optional JMS-style selector expression (may be null);
     *                      ignored on subsequent calls if the receiver already exists
     * @return the received message, or null on timeout
     * @throws JCSMPException if no transacted session is active, or flow setup / receive fails
     */
    public BytesXMLMessage pollTransacted(String queueName, long timeoutMillis, String selector)
            throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("pollTransacted called but no transacted session active on connection: "
                        + connectionId);
                throw new JCSMPException(
                        "pollTransacted called but no transacted session active on connection: "
                        + connectionId);
            }

            String normalizedSelector = (selector == null || selector.isEmpty()) ? null : selector;
            TxFlowEntry entry = txFlowReceivers.get(queueName);
            if (entry == null) {
                Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
                ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
                flowProps.setEndpoint(queue);
                flowProps.setStartState(true);
                if (normalizedSelector != null) {
                    flowProps.setSelector(normalizedSelector);
                }
                EndpointProperties endpointProps = new EndpointProperties();
                endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

                log.debug("pollTransacted: creating transacted flow for queue '" + queueName
                        + "' on connectionId=" + connectionId
                        + (normalizedSelector != null ? " (selector='" + normalizedSelector + "')" : ""));
                // Settlement on a transacted flow is implicit (commit/rollback), so we do NOT
                // set CLIENT_ACK mode here — that's only for the non-transacted poll path.
                FlowReceiver receiver = txSession.createFlow(null, flowProps, endpointProps);
                entry = new TxFlowEntry(receiver, normalizedSelector);
                txFlowReceivers.put(queueName, entry);
            } else if (!java.util.Objects.equals(entry.selector, normalizedSelector)) {
                // JCSMP binds the selector at flow-creation time. Silently ignoring the new
                // selector and reusing a flow with a different filter would be a correctness
                // bug — reject the call so the caller can split this into a separate
                // transaction (one selector per queue per tx).
                throw new JCSMPException(
                        "pollTransacted: selector mismatch on queue '" + queueName + "' (existing='"
                        + entry.selector + "', requested='" + normalizedSelector + "'). A queue's"
                        + " transacted flow is created once per transaction with a fixed selector;"
                        + " use only one selector per queue within a single transaction.");
            }

            return entry.receiver.receive((int) timeoutMillis);
        }
    }

    /**
     * Bookkeeping for a transacted FlowReceiver — pairs the receiver with the selector
     * it was created with so we can reject mismatched reuses inside the same transaction.
     */
    private static final class TxFlowEntry {
        final FlowReceiver receiver;
        final String selector;
        TxFlowEntry(FlowReceiver receiver, String selector) {
            this.receiver = receiver;
            this.selector = selector;
        }
    }

    /**
     * Synchronously polls one message from a queue and acknowledges it on receipt.
     * Provisions a short-lived FlowReceiver on the queue, waits up to {@code timeoutMillis}
     * for a message, acks it (so the broker removes it from the queue), and tears the flow
     * down before returning.
     *
     * <p><b>Settlement is atomic with consumption</b> — manual ack/nack of polled messages
     * is not supported because the flow closes before mediation continues. For atomic
     * consume-then-publish or rollback-on-failure semantics, wrap publish operations in
     * {@code beginTransaction} / {@code commit} (consumer-side transactional support).</p>
     *
     * @param queueName     the durable queue to poll
     * @param timeoutMillis maximum time to block waiting for a message
     * @param selector      optional JMS-style selector expression (may be null)
     * @return the received message, or null on timeout
     * @throws JCSMPException if flow setup or receive fails
     */
    public BytesXMLMessage pollMessage(String queueName, long timeoutMillis, String selector)
            throws JCSMPException {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(queue);
        flowProps.setStartState(true);
        flowProps.setAckMode(com.solacesystems.jcsmp.JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        if (selector != null && !selector.isEmpty()) {
            flowProps.setSelector(selector);
        }

        EndpointProperties endpointProps = new EndpointProperties();
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

        FlowReceiver flowReceiver = null;
        try {
            flowReceiver = session.createFlow(null, flowProps, endpointProps);
            BytesXMLMessage message = flowReceiver.receive((int) timeoutMillis);
            if (message != null) {
                message.ackMessage();
            }
            return message;
        } finally {
            if (flowReceiver != null) {
                flowReceiver.close();
            }
        }
    }

    /**
     * Browses messages on a queue without consuming them. Uses the JCSMP {@link Browser}
     * API. The browser inspects the message-spool replicator without removing messages,
     * so the same messages remain available to live consumers.
     *
     * @param queueName     the queue to browse
     * @param maxMessages   maximum messages to collect; the loop stops earlier if no
     *                      message arrives within {@code timeoutMillis}
     * @param timeoutMillis per-message wait — how long to block between successive
     *                      {@code getNext} calls before declaring the browse done
     * @param selector      optional JMS-style selector expression (may be null)
     * @return a list of browsed messages (never null, may be empty)
     * @throws JCSMPException if browser creation fails
     */
    public List<BytesXMLMessage> browseQueue(String queueName, int maxMessages, long timeoutMillis,
                                             String selector) throws JCSMPException {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        BrowserProperties browserProps = new BrowserProperties();
        browserProps.setEndpoint(queue);
        if (selector != null && !selector.isEmpty()) {
            browserProps.setSelector(selector);
        }

        Browser browser = null;
        List<BytesXMLMessage> messages = new ArrayList<>();
        try {
            browser = session.createBrowser(browserProps);
            // getNext blocks up to timeoutMillis. A null return means "nothing more right now",
            // which is the natural stop condition for a one-shot browse — keep simple.
            int browseTimeoutInt = (int) Math.min(timeoutMillis, Integer.MAX_VALUE);
            for (int i = 0; i < maxMessages; i++) {
                BytesXMLMessage msg = browser.getNext(browseTimeoutInt);
                if (msg == null) {
                    break;
                }
                messages.add(msg);
            }
            return messages;
        } finally {
            if (browser != null) {
                browser.close();
            }
        }
    }

    /**
     * DISABLED / NOT YET RELEASED: this method backs the requestCachedMessages operation,
     * which is intentionally hidden from the operation palette (its component is left
     * unregistered in consume/component.xml). It is retained here, compiled but unreachable,
     * and will be released once the operation has been tested end-to-end against a real
     * PubSub+ Cache instance.
     *
     * <p>Issues a Solace Cache request for a topic pattern. Adds a transient subscription on
     * the session, sends a blocking cache request to the named cache instance, then drains
     * any cached messages that arrived on the session's sync consumer. The subscription is
     * removed before returning.
     *
     * <p><b>Prerequisites — this operation only works when the broker side is set up:</b></p>
     * <ul>
     *   <li>A PubSub+ Cache Instance is deployed and running, and joined to the Message VPN.</li>
     *   <li>The cache instance is configured to cache the requested topic pattern (cache
     *       instances cache only topics they have been subscribed to administratively).</li>
     *   <li>The cache instance name passed here matches the broker's cache instance name.</li>
     *   <li>The connecting client has authorization to issue cache requests on the VPN.</li>
     * </ul>
     * <p>If any of these is missing, the broker returns an error and this call throws —
     * the connector cannot detect the misconfiguration ahead of time.</p>
     *
     * @param cacheName        the cache-instance name configured on the broker
     * @param topicPattern     the topic (or wildcard pattern) to request cached messages for
     * @param maxMessages      max messages to request from cache (0 = unlimited per Solace)
     * @param maxAgeSeconds    max age of cached messages in seconds (0 = any age)
     * @param requestTimeoutMs how long the cache request itself may take
     * @param liveDataAction   one of FULFILL, FLOW_THRU, QUEUE — controls how live messages
     *                         on the same topic interleave with the cached delivery. For a
     *                         wildcard {@code topicPattern} (containing '*' or '>'), only
     *                         FLOW_THRU is valid; FULFILL and QUEUE require an exact topic
     * @param drainTimeoutMs   how long to keep draining the consumer for cached messages
     *                         after the cache request returns. Cached messages can arrive
     *                         asynchronously on the session consumer; this is the wait window
     * @return list of received cached messages
     * @throws JCSMPException if cache session creation, subscription, or request fails
     * @throws IllegalArgumentException if a wildcard topic is combined with a live data action
     *                                  other than FLOW_THRU
     */
    public List<BytesXMLMessage> requestCachedMessages(String cacheName, String topicPattern,
                                                       long maxMessages, int maxAgeSeconds,
                                                       long requestTimeoutMs, String liveDataAction,
                                                       long drainTimeoutMs) throws JCSMPException {

        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicPattern);
        CacheLiveDataAction action = resolveLiveDataAction(liveDataAction);

        // Solace constraint: a cache request whose topic contains wildcards ('*' or '>') only
        // supports the FLOW_THRU live data action. FULFILL and QUEUE require an exact topic, and
        // JCSMP rejects the combination deep inside CacheRequestProperties' constructor with
        // "Wildcard topic and live data action <ACTION> not supported". Fail fast here with a
        // clear, actionable message instead of surfacing that raw IllegalArgumentException.
        if ((topicPattern.contains("*") || topicPattern.contains(">"))
                && action != CacheLiveDataAction.FLOW_THRU) {
            throw new IllegalArgumentException(
                    "Cache request for wildcard topic '" + topicPattern + "' requires liveDataAction"
                    + " FLOW_THRU; '" + action + "' is only valid for an exact (non-wildcard) topic."
                    + " Set liveDataAction to FLOW_THRU, or request an exact topic.");
        }

        // CacheSessionProperties has no no-arg constructor; use the 4-arg form
        // (name, maxMsgs, maxAge, timeout). Timeout is per-cache-request in ms.
        CacheSessionProperties cacheProps = new CacheSessionProperties(
                cacheName,
                (int) Math.min(maxMessages, Integer.MAX_VALUE),
                maxAgeSeconds,
                (int) Math.min(requestTimeoutMs, Integer.MAX_VALUE));

        CacheSession cacheSession = session.createCacheSession(cacheProps);
        List<BytesXMLMessage> collected = new ArrayList<>();
        boolean subscribed = false;
        try {
            // Cached messages are delivered to the session consumer
            ensureConsumer();

            // The session consumer is shared and long-lived across pooled reuse. A previous
            // operation (e.g. an earlier cache request that hit its message cap or quiet-period
            // break) may have left messages buffered on it. Clear them first so they can't be
            // mis-attributed to this cache request.
            int stale = drainSessionConsumer();
            if (stale > 0) {
                log.warn("requestCachedMessages: discarded " + stale + " stale message(s) buffered on"
                        + " the session consumer before cache request for topic '" + topicPattern + "'");
            }

            // Subscribe so cached messages route to the session consumer we drain below.
            // On a pooled session the subscription may already be present (subcode 13);
            // tolerate it and reuse it — finally still removes it.
            try {
                session.addSubscription(topic);
            } catch (JCSMPErrorResponseException e) {
                if (e.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                    throw e;
                }
                log.debug("Subscription for topic '" + topicPattern + "' already present on the pooled"
                        + " session; reusing it for this cache request.");
            }
            subscribed = true;

            // Blocking cache request — returns when broker finishes streaming cached msgs
            // or the request times out. subscribeFlag MUST be false: we add the subscription
            // ourselves above. Passing true makes JCSMP add the SAME subscription a second time,
            // which the broker rejects with "400: Subscription Already Exists" (subcode 13).
            CacheRequestResult result = cacheSession.sendCacheRequest(
                    Long.valueOf(System.nanoTime()), topic, false, action);
            if (log.isDebugEnabled()) {
                log.debug("Cache request to '" + cacheName + "' for topic '" + topicPattern
                        + "' returned: " + result);
            }

            // Cached messages flow to the session consumer asynchronously. Drain it for the
            // configured window — receive(timeout) returns null when no message is ready.
            long deadline = System.currentTimeMillis() + drainTimeoutMs;
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                BytesXMLMessage msg = consumer.receive((int) Math.min(remaining, 500));
                if (msg == null) {
                    // A null after some collected messages typically means cache delivery is done;
                    // break to avoid burning the full drain window when there's nothing left.
                    if (!collected.isEmpty()) {
                        break;
                    }
                    continue;
                }
                collected.add(msg);
                if (maxMessages > 0 && collected.size() >= maxMessages) {
                    break;
                }
            }
            return collected;
        } finally {
            if (subscribed) {
                try {
                    session.removeSubscription(topic);
                } catch (JCSMPException e) {
                    log.warn("Failed to remove transient cache subscription for topic '"
                            + topicPattern + "': " + e.getMessage());
                }
            }
            try {
                // Release the per-request cache session created for this operation.
                cacheSession.close();
            } catch (Exception ignored) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing cache session during cleanup for topic '" + topicPattern
                            + "' on connection: " + connectionId + " — " + ignored.getMessage());
                }
            }
            // Drain anything still buffered (messages that arrived after we hit the cap or the
            // quiet-period break) so no residue is left on the shared consumer for the next
            // borrower of this pooled connection.
            int leftover = drainSessionConsumer();
            if (leftover > 0 && log.isDebugEnabled()) {
                log.debug("requestCachedMessages: drained " + leftover + " trailing message(s) after"
                        + " cache request for topic '" + topicPattern + "'");
            }
        }
    }

    /**
     * Lazily creates and starts the session's synchronous consumer the first time an
     * operation needs to receive on the session — {@link #requestCachedMessages} (cached
     * messages are delivered here) or the DIRECT branch of {@link #sendRequest} (the
     * {@link Requestor} reads its reply here). Connections that only publish/poll/browse
     * never create one. Idempotent and synchronized so reuse across pooled borrows returns
     * the same started consumer.
     */
    private XMLMessageConsumer ensureConsumer() throws JCSMPException {
        synchronized (consumerLock) {
            if (consumer == null) {
                XMLMessageConsumer newConsumer = null;
                try {
                    // The guard registered at connect is the consumer to start, so the reconnect handler stays
                    // with the session (A45). Without one, a null listener gives the synchronous consumer as today.
                    XMLMessageConsumer guard = handler.guard();
                    newConsumer = guard != null ? guard : session.getMessageConsumer((XMLMessageListener) null);
                    newConsumer.start();
                    // Publish to the field only once fully started, so a start() failure can't
                    // leave a half-initialized consumer that later calls would blindly reuse.
                    consumer = newConsumer;
                    if (log.isDebugEnabled()) {
                        log.debug("Session consumer started on connection: " + connectionId);
                    }
                } catch (JCSMPException e) {
                    // Close the partially-created consumer so it isn't leaked and the next call
                    // retries from a clean state. Don't log here — the operation layer
                    // (SolaceRequestCachedMessages / SolaceSendRequest) logs and reports this as
                    // a connector fault. Rethrow with context so that single log says where it
                    // failed, instead of duplicate-logging the same error at this hop.
                    if (newConsumer != null) {
                        try {
                            newConsumer.close();
                        } catch (Exception ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Error closing partially-created consumer on connection: "
                                        + connectionId + " — " + ignored.getMessage());
                            }
                        }
                    }
                    // The next call creates a fresh consumer (A45).
                    handler.clearGuard();
                    throw new JCSMPException("Failed to create session consumer on connection "
                            + connectionId, e);
                }
            }
            return consumer;
        }
    }

    /**
     * Drains and discards every message currently buffered on the shared session consumer,
     * returning the count discarded. Non-blocking ({@code receiveNoWait}) — it only clears
     * what is already queued, not what might arrive later. Used to keep cached-message
     * residue from bleeding across pooled reuse of this connection (the session consumer is
     * shared by the cache-request and DIRECT request-reply paths). Best-effort: a receive
     * error just ends the drain. Returns 0 if the consumer was never created.
     */
    private int drainSessionConsumer() {
        synchronized (consumerLock) {
            if (consumer == null) {
                return 0;
            }
            int discarded = 0;
            try {
                while (consumer.receiveNoWait() != null) {
                    discarded++;
                }
            } catch (JCSMPException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error draining session consumer (stopping drain): " + e.getMessage());
                }
            }
            return discarded;
        }
    }

    private CacheLiveDataAction resolveLiveDataAction(String value) {
        if (value == null) {
            return CacheLiveDataAction.FULFILL;
        }
        switch (value.toUpperCase()) {
            case SolaceConstants.CACHE_LIVE_DATA_FLOW_THRU:
                return CacheLiveDataAction.FLOW_THRU;
            case SolaceConstants.CACHE_LIVE_DATA_QUEUE:
                return CacheLiveDataAction.QUEUE;
            case SolaceConstants.CACHE_LIVE_DATA_FULFILL:
            default:
                return CacheLiveDataAction.FULFILL;
        }
    }

    /**
     * Submit only. Nothing here blocks on the broker, so it is legal under the per name lock and inside
     * destroyObject. Everything is captured under the lock and nulled at once, and the one closer task reads
     * only its captured copies, in the 1.0.1 order.
     */
    public void disconnect() {
        XMLMessageConsumer consumerToClose;
        XMLMessageProducer producerToClose;
        JCSMPSession sessionToClose;
        TransactedSession transactedSession;
        XMLMessageProducer transactedProducer;
        Map<String, TxFlowEntry> transactedFlows;
        synchronized (txLock) {
            consumerToClose = consumer;
            consumer = null;
            producerToClose = producer;
            producer = null;
            sessionToClose = session;
            session = null;
            transactedSession = txSession;
            txSession = null;
            transactedProducer = txProducer;
            txProducer = null;
            transactedFlows = new HashMap<>(txFlowReceivers);
            txFlowReceivers.clear();
        }
        // Completes the pending acknowledgement futures exceptionally, inline, no broker call.
        publisher.failPending("Connection " + connectionId + " closed before publish ACK was received");
        lc.closer.execute(() -> {
            try {
                if (consumerToClose != null) {
                    consumerToClose.close();
                }
            } catch (Exception e) {
                log.debug("Solace connection " + connectionId + ": consumer close failed", e);
            }
            try {
                if (producerToClose != null) {
                    producerToClose.close();
                }
            } catch (Exception e) {
                log.debug("Solace connection " + connectionId + ": producer close failed", e);
            }
            for (Map.Entry<String, TxFlowEntry> entry : transactedFlows.entrySet()) {
                try {
                    entry.getValue().receiver.close();
                } catch (Exception ex) {
                    log.warn("Error closing transacted flow receiver for queue '" + entry.getKey()
                            + "' on connection " + connectionId + ": " + ex.getMessage());
                }
            }
            try {
                if (transactedProducer != null) {
                    transactedProducer.close();
                }
            } catch (Exception e) {
                log.debug("Solace connection " + connectionId + ": transacted producer close failed", e);
            }
            try {
                if (transactedSession != null) {
                    transactedSession.close();
                }
            } catch (Exception e) {
                log.debug("Solace connection " + connectionId + ": transacted session close failed", e);
            }
            if (sessionToClose != null) {
                Hooks.get().point("closer.beforeClose", sessionToClose);
                SolaceLogicalConnection.closeQuietly(sessionToClose);
                log.info("Solace connection closed: " + connectionId);
            }
        });
    }

    @Override
    public void connect(ConnectionConfig connectionConfig) throws ConnectException {
        // Connection is established in the constructor
    }

    @Override
    public void close() throws ConnectException {
        orphan();
    }
}
