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
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.models.PublishResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static org.wso2.integration.connector.constants.SolaceConstants.ACK_STATUS_ACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ACK_STATUS_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ACK_STATUS_NOT_APPLICABLE;
import static org.wso2.integration.connector.constants.SolaceConstants.ACK_STATUS_NOT_REQUESTED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_DEADLINE_BEFORE_SEND;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE_BEFORE_SEND;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;

/**
 * The producer lock, the pending acknowledgements and the deadline bounded send algorithm of one session
 * (guide 3.4). One per pooled SolaceConnection and one per RequestReplySession, rebuilt with the session.
 */
public final class SessionPublisher {

    private static final Log log = LogFactory.getLog(SessionPublisher.class);

    private final SolaceLogicalConnection lc;
    // The log identity of the owning session.
    private final String ownerId;
    volatile JCSMPSession session;
    volatile XMLMessageProducer producer;
    // Covers send() only above a publisher window of 1. At the default window of 1 it is held through the
    // confirmation wait, because JCSMP serialises the sends there anyway and a thread must not be cut inside
    // send() (design 7.10, A59).
    final ReentrantLock producerLock = new ReentrantLock();
    final ConcurrentMap<String, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();
    private final PublishEventHandler eventHandler = new PublishEventHandler();

    SessionPublisher(SolaceLogicalConnection lc, String ownerId) {
        this.lc = lc;
        this.ownerId = ownerId;
    }

    /** Replaces both and fails every pending future, because no callback of the old producer will arrive. */
    void swap(JCSMPSession newSession, XMLMessageProducer newProducer) {
        session = newSession;
        producer = newProducer;
        failPending("Connection " + ownerId + " closed before publish ACK was received");
    }

    void failPending(String reason) {
        if (pendingAcks.isEmpty()) {
            return;
        }
        JCSMPException closing = new JCSMPException(reason);
        for (CompletableFuture<Void> future : pendingAcks.values()) {
            future.completeExceptionally(closing);
        }
        pendingAcks.clear();
    }

    JCSMPStreamingPublishCorrelatingEventHandler eventHandler() {
        return eventHandler;
    }

    /**
     * Operation thread. The caller has attached op to this publisher's session.
     * request is null, or the request for sendReply.
     */
    public PublishResult send(PublishOperation op, BytesXMLMessage msg, Destination dest, XMLMessage request,
                              boolean waitForAck) throws SolaceFault {
        boolean guaranteed = msg.getDeliveryMode() != DeliveryMode.DIRECT;
        String key = null;
        CompletableFuture<Void> future = null;
        try {
            if (guaranteed) {
                key = MessageBuilder.newCorrelationKey(dest != null ? dest.getName() : "reply");
                msg.setCorrelationKey(key);
                if (waitForAck) {
                    // The handler completes inner. The test build may delay the outer future the waiter sees,
                    // so the JCSMP callback never blocks.
                    CompletableFuture<Void> inner = new CompletableFuture<>();
                    pendingAcks.put(key, inner);
                    future = Hooks.get().wrapConfirmation(inner);
                }
            }
            long remaining = op.remaining();
            if (remaining <= 0 || !producerLock.tryLock(remaining, TimeUnit.MILLISECONDS)) {
                // Safe: nothing was sent.
                throw new SolaceFault(FAULT_DEADLINE_BEFORE_SEND, ERROR_CODE_DEADLINE_BEFORE_SEND,
                        "deadline passed before send() (" + op.id + ")");
            }
            if (!op.enterStage(PublishOperation.Stage.SENDING)) {
                // The recheck after the lock and the SENDING mark are one step under the record's monitor, so no
                // send starts after an expiry that left the session alone. Still safe, nothing was sent (A51).
                producerLock.unlock();
                throw new SolaceFault(FAULT_DEADLINE_BEFORE_SEND, ERROR_CODE_DEADLINE_BEFORE_SEND,
                        "expired while waiting for the producer lock (" + op.id + ")");
            }
            boolean holdThroughConfirm = false;
            try {
                Hooks.get().point("publisher.beforeSend", op);
                if (request != null) {
                    producer.sendReply(request, msg);
                } else {
                    producer.send(msg, dest);
                }
                op.enterStage(PublishOperation.Stage.SENT);
                // At window 1 the lock is held through the confirmation wait, so a send never blocks inside JCSMP
                // on window space and every wait under contention is the lock wait that A51 made safe (A59).
                // Above 1 the hold is the T32 regression toggle only.
                holdThroughConfirm = future != null
                        && (lc.settings.pubAckWindowSize == 1
                                || Boolean.TRUE.equals(Hooks.get().point("publisher.holdLockThroughConfirm")));
            } finally {
                // Released as soon as send() returns, so confirmation waits overlap above a window of 1.
                if (!holdThroughConfirm) {
                    producerLock.unlock();
                }
            }
            try {
                if (future == null) {
                    if (!op.complete()) {
                        throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED, "operation expired (" + op.id + ")",
                                null, guaranteed);
                    }
                    return new PublishResult(guaranteed ? ACK_STATUS_NOT_REQUESTED : ACK_STATUS_NOT_APPLICABLE, false,
                            key, null);
                }
                Hooks.get().point("publisher.beforeConfirmWait", op);
                try {
                    future.get(Math.max(1, op.remaining()), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    // The caller's own timed wait can fire before the watchdog callback. Same transition.
                    op.expire();
                    throw new SolaceFault(FAULT_DEADLINE, ERROR_CODE_DEADLINE,
                            "no broker acknowledgement within the deadline (" + op.id + ")", null, true);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    op.expire();
                    throw new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                            "interrupted while waiting for the broker acknowledgement (" + op.id + ")", e, true);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (!(cause instanceof JCSMPErrorResponseException)) {
                        // A transport error, or the local close reported by failPending. Not a broker rejection,
                        // so the outcome is uncertain and the session is discarded.
                        op.expire();
                        throw new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                                "no confirmed result after send(): " + cause, cause, true);
                    }
                    if (!op.complete()) {
                        throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED, "operation expired (" + op.id + ")",
                                null, true);
                    }
                    // A real broker NACK is a known completed failure. The session stays usable.
                    JCSMPErrorResponseException rejection = (JCSMPErrorResponseException) cause;
                    String detail = "subcode " + rejection.getSubcodeEx() + ", " + rejection.getResponsePhrase();
                    lc.counters.publishNacked.increment();
                    Hooks.get().record("publish.startToConfirmation", System.nanoTime() - op.registeredNanos);
                    return new PublishResult(ACK_STATUS_NACK, false, key, detail);
                }
                if (!op.complete()) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED, "operation expired (" + op.id + ")", null,
                            true);
                }
                Hooks.get().record("publish.startToConfirmation", System.nanoTime() - op.registeredNanos);
                return new PublishResult(ACK_STATUS_ACK, true, key, null);
            } finally {
                // The late release of the hold.
                if (holdThroughConfirm) {
                    producerLock.unlock();
                }
            }
        } catch (SolaceFault fault) {
            throw fault;
        } catch (InterruptedException e) {
            // Only tryLock reaches here. Nothing was sent and the flag is re asserted (guide 0.6 rule 10).
            Thread.currentThread().interrupt();
            op.fail();
            throw new SolaceFault(FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR,
                    "interrupted before send() (" + op.id + ")", e, false);
        } catch (Exception e) {
            if (op.stage() == PublishOperation.Stage.BEFORE_SEND) {
                // Safe: the session is returned usable.
                op.fail();
                throw new SolaceFault(FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR,
                        "publish failed before send(): " + e.getMessage(), e, false);
            }
            // No JCSMP exception class proves nothing was submitted. Uncertain, session discarded.
            op.expire();
            throw new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                    "exception inside or after send(): " + e.getMessage(), e, true);
        } finally {
            if (key != null) {
                pendingAcks.remove(key);
            }
        }
    }

    /**
     * Handler for asynchronous publish events (acks and errors). Non-static so it can
     * signal the {@link #pendingAcks} futures owned by the surrounding connection —
     * required for the synchronous {@code waitForAck} path in {@link #publish}.
     */
    private class PublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

        @Override
        public void responseReceivedEx(Object correlationKey) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.complete(null);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Publish ACK received. CorrelationKey: " + correlationKey);
            }
        }

        @Override
        public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.completeExceptionally(cause);
                }
            }
            if (cause instanceof JCSMPErrorResponseException) {
                JCSMPErrorResponseException errResp = (JCSMPErrorResponseException) cause;
                log.error("Publish NACK - broker rejected message. CorrelationKey: " + correlationKey
                        + ", subcode: " + errResp.getSubcodeEx()
                        + ", responseCode: " + errResp.getResponseCode()
                        + ", info: " + errResp.getResponsePhrase(), cause);
            } else if (cause instanceof JCSMPTransportException) {
                log.error("Publish error - transport failure. CorrelationKey: " + correlationKey
                        + ", cause: " + cause.getMessage(), cause);
            } else {
                log.error("Publish error. CorrelationKey: " + correlationKey, cause);
            }
        }
    }
}
