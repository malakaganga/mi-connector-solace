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

package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.LogicalConnections;
import org.wso2.integration.connector.connection.PollFlowOwner;
import org.wso2.integration.connector.connection.PollLease;
import org.wso2.integration.connector.connection.PollRequest;
import org.wso2.integration.connector.connection.PollSession;
import org.wso2.integration.connector.connection.PublishOperation;
import org.wso2.integration.connector.connection.SolaceLogicalConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.connection.TransactionBridge;
import org.wso2.integration.connector.connection.TransactionCommand;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.wso2.integration.connector.constants.SolaceConstants.ACK_MODE_AUTO;
import static org.wso2.integration.connector.constants.SolaceConstants.ACK_MODE_CLIENT;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_UNSETTLED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_UNSETTLED;

/**
 * Synchronously polls one message from a Solace queue. Returns null on timeout.
 * The polled message's payload becomes the response variable's payload (or replaces
 * the message body if {@code overwriteBody} is true). Metadata such as messageId,
 * correlationId, and the received/timedOut flags are exposed as response attributes
 * (accessible via {@code ${vars.X.attributes.messageId}}) and as legacy
 * {@code solace.*} message-context properties.
 *
 * <p>Settlement in 2.0.0. Outside a transaction the poll runs on a cached flow owned by
 * one thread per queue and selector. With ackMode=AUTO the owner acknowledges the message right after it handed
 * the message to this call, at most once. With ackMode=CLIENT the owner's lease is stashed under
 * solace.poll.message and the sequence settles it with acknowledgeMessage or nackMessage and target=POLL before
 * pollAckTimeout passes, otherwise the connector settles it FAILED and the broker redelivers it. Inside a
 * connector owned transaction the 1.0.1 transacted poll applies: the broker holds the message until commit or
 * rollback. A poll inside an inbound owned transaction is refused.</p>
 */
public class SolacePollMessage extends AbstractConnectorOperation {

    private static final Log log = LogFactory.getLog(SolacePollMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        // Detect transactional context the same way publish does — if the caller wrapped
        // this poll in beginTransaction, route to the transacted path so settlement is
        // deferred to commit/rollback (rollback redelivers; non-transacted poll loses).
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        boolean isTransactional = (txId != null);
        if (isTransactional) {
            log.debug("solace.poll: transactional path, txId=" + txId
                    + ", connectionName=" + connectionName);
        }

        // A poll inside an inbound owned transaction has no defined settlement.
        if (messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX) instanceof Executor) {
            fault(FAULT_INBOUND_TRANSACTION, ERROR_CODE_INBOUND_TRANSACTION,
                    "poll is not supported inside an inbound-owned transaction (D22)", null, messageContext);
            return;
        }

        String queueName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.QUEUE_NAME);
        if (StringUtils.isEmpty(queueName)) {
            handleException("Queue name is required for poll operation.", messageContext);
            return;
        }

        String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.POLL_TIMEOUT);
        long timeout = SolaceUtils.parseLongOrDefault(timeoutStr,
                SolaceConstants.DEFAULT_POLL_TIMEOUT_MS, SolaceConstants.POLL_TIMEOUT);

        String selector = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SELECTOR);
        // An empty selector means no selector, as the 1.0.1 flow creation read it.
        if (StringUtils.isEmpty(selector)) {
            selector = null;
        }

        if (isTransactional) {
            // Connector owned: the 1.0.1 transacted poll, now always on the transaction's executor thread.
            final SolaceTransactionRegistry.Entry entry = SolaceTransactionRegistry.get(txId);
            if (entry == null) {
                log.error("solace.poll: txId=" + txId + " not found in TransactionRegistry");
                handleException("Transaction " + txId + " not found", messageContext);
                return;
            }
            log.info("solace.poll: txId=" + txId
                    + " resolved to connectionId=" + entry.connection.getConnectionId());

            if (log.isDebugEnabled()) {
                log.debug("Polling queue '" + queueName + "' (timeout=" + timeout + "ms"
                        + (selector != null ? ", selector='" + selector + "'" : "")
                        + ", transactional=" + isTransactional + ")");
            }
            final String pollSelector = selector;
            try {
                BytesXMLMessage message = TransactionBridge.submitAndWait(entry.executor,
                        new TransactionCommand<>(() -> entry.connection.pollTransacted(queueName, timeout, pollSelector),
                                timeout),
                        "poll", FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, messageContext);
                writeResult(messageContext, message, queueName, responseVariable, overwriteBody, null, null);
            } catch (SolaceFault fault) {
                fault(fault, messageContext, responseVariable);
            } catch (Exception e) {
                fault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                        "Solace poll operation failed (connection: " + connectionName + ")", e, messageContext);
            }
            return;
        }

        String ackModeRaw = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.ACK_MODE);
        String ackMode = StringUtils.isEmpty(ackModeRaw) ? ACK_MODE_AUTO : ackModeRaw.trim().toUpperCase(Locale.ROOT);
        if (!ACK_MODE_AUTO.equals(ackMode) && !ACK_MODE_CLIENT.equals(ackMode)) {
            fault(FAULT_INVALID_CONFIG, ERROR_CODE_INVALID_CONFIG,
                    "ackMode must be AUTO or CLIENT, got '" + ackModeRaw + "'", null, messageContext);
            return;
        }
        // Before any receive: a second CLIENT poll while a polled message is still unsettled.
        Object stashed = messageContext.getProperty(SolaceConstants.SOLACE_POLL_MESSAGE);
        if (ACK_MODE_CLIENT.equals(ackMode) && stashed instanceof PollLease
                && ((PollLease) stashed).state == PollLease.OUTSTANDING) {
            fault(FAULT_POLL_UNSETTLED, ERROR_CODE_POLL_UNSETTLED,
                    "a polled message is still unsettled in this message context; settle it with acknowledgeMessage or nackMessage (target=POLL) before polling again",
                    null, messageContext);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Polling queue '" + queueName + "' (timeout=" + timeout + "ms"
                    + (selector != null ? ", selector='" + selector + "'" : "")
                    + ", ackMode=" + ackMode + ")");
        }

        SolaceLogicalConnection lc = LogicalConnections.get(connectionName);
        if (lc == null) {
            fault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + connectionName + "' is not initialised", null, messageContext);
            return;
        }

        // No pooled connection is borrowed on this path.
        PollRequest request = null;
        try {
            // The phase A record of the poll timeout. It bounds the poll session's first connect only and
            // faults CONNECTION_ERROR at once on a closed connection.
            PublishOperation setup = PublishOperation.register(lc, timeout);
            PollSession pollSession;
            try {
                // Lazily created, rebuilt on the next use after DOWN_ERROR.
                pollSession = lc.pollSession(setup);
            } finally {
                // The record ends here, before the bind and the request. Both are bounded by the session's
                // readTimeout and the request deadline.
                setup.finish();
            }
            // May fault POLL_FLOW_LIMIT or INVALID_CONFIG. Binds on this thread.
            PollFlowOwner owner = pollSession.ownerFor(queueName, selector);
            request = new PollRequest(System.currentTimeMillis() + timeout, ackMode);
            // The timeout covers the wait for the owner's turn as well as the receive.
            owner.submit(request);
            BytesXMLMessage message;
            PollLease lease;
            synchronized (request) {
                long remaining;
                while (request.state == PollRequest.QUEUED
                        && (remaining = request.deadline - System.currentTimeMillis()) > 0) {
                    request.wait(remaining);
                }
                if (request.state == PollRequest.DONE) {
                    // Late or not: the lease is already live.
                    message = request.result;
                    lease = request.lease;
                } else {
                    // The owner will never deliver to this request.
                    request.state = PollRequest.EXPIRED;
                    message = null;
                    lease = null;
                }
            }
            // C.5
            Hooks.get().record("poll.requestToResult", System.nanoTime() - request.createdNanos);
            if (lease != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_POLL_MESSAGE, lease);
            }
            writeResult(messageContext, message, queueName, responseVariable, overwriteBody, ackMode, lease);
        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
        } catch (InterruptedException e) {
            // The wait on the request is interruptible. The interrupt is the caller's expiry: the request is
            // marked EXPIRED under its monitor so the owner never delivers to it, and the flag is re asserted.
            Thread.currentThread().interrupt();
            if (request != null) {
                synchronized (request) {
                    if (request.state == PollRequest.QUEUED) {
                        request.state = PollRequest.EXPIRED;
                    }
                }
            }
            fault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, "interrupted while waiting for the poll flow owner", e,
                    messageContext);
        } catch (JCSMPException e) {
            fault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, "Solace poll failed: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            fault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                    "Solace poll operation failed (connection: " + connectionName + ")", e, messageContext);
        }
    }

    /**
     * Builds the response payload + attributes and routes them through
     * {@link #handleConnectorResponse} so {@code ${vars.X}} resolves correctly and
     * {@code overwriteBody} behaves identically to other framework operations.
     */
    private void writeResult(MessageContext messageContext, BytesXMLMessage message, String queueName,
                             String responseVariable, Boolean overwriteBody, String ackMode, PollLease lease) {

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(SolaceConstants.RESULT_DESTINATION, queueName);
        attributes.put(SolaceConstants.RESULT_RECEIVED, message != null);
        attributes.put(SolaceConstants.RESULT_TIMED_OUT, message == null);
        if (ackMode != null) {
            attributes.put("ackMode", ackMode);
        }
        if (lease != null) {
            attributes.put("leaseDeadline", lease.deadline);
        }

        String payloadJson;

        if (message == null) {
            // Timeout path. Use a JSON envelope so callers can branch on receivedflag
            // without dereferencing a null payload.
            JSONObject envelope = new JSONObject();
            envelope.put(SolaceConstants.RESULT_RECEIVED, false);
            envelope.put(SolaceConstants.RESULT_TIMED_OUT, true);
            envelope.put(SolaceConstants.RESULT_DESTINATION, queueName);
            payloadJson = envelope.toString();
        } else {
            // Surface common metadata in legacy solace.* properties for downstream mediation.
            if (message.getMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_MESSAGE_ID, message.getMessageId());
            }
            if (message.getCorrelationId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_CORRELATION_ID, message.getCorrelationId());
            }
            if (message.getApplicationMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID,
                        message.getApplicationMessageId());
            }
            if (message.getApplicationMessageType() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                        message.getApplicationMessageType());
            }
            if (message.getSenderId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_SENDER_ID, message.getSenderId());
            }
            if (message.getDeliveryMode() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE,
                        message.getDeliveryMode().name());
            }
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, queueName);
            messageContext.setProperty(SolaceConstants.SOLACE_REDELIVERED, message.getRedelivered());

            // Standard SMF headers + publisher-supplied userProperties — surfaced as
            // ${vars.X.attributes.*}. Done before contentType so the inferred content
            // type takes precedence over anything the publisher may have put there.
            SolaceUtils.populateMessageMetadata(message, attributes);

            byte[] bytes = SolaceUtils.extractRawPayload(message);
            String contentType = message.getHTTPContentType();
            if (StringUtils.isEmpty(contentType)) {
                contentType = SolaceUtils.inferContentType(message);
            }
            attributes.put("contentType", contentType);

            // The payload becomes either the body (when overwriteBody=true) or the
            // payload field of the response variable. handleConnectorResponse parses it
            // as JSON, so wrap non-JSON content in a JSON string literal.
            String body = new String(bytes, StandardCharsets.UTF_8);
            payloadJson = SolaceUtils.looksLikeJson(body) ? body : JSONObject.quote(body);
        }

        handleConnectorResponse(messageContext, responseVariable, overwriteBody, payloadJson,
                null, attributes);
    }

    private void fault(SolaceFault fault, MessageContext messageContext, String responseVariable) {
        if (fault.outcomeUnknown) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("outcome", SolaceConstants.OUTCOME_UNKNOWN);
            handleConnectorResponse(messageContext, responseVariable, false, new JSONObject(attributes).toString(),
                    null, attributes);
        }
        fault(fault.name, fault.code, fault.getMessage(), fault.getCause(), messageContext);
    }

    private void fault(String name, int code, String message, Throwable cause, MessageContext messageContext) {
        String text = name + ": " + message;
        ConnectorUtils.setErrorPropertiesToMessage(messageContext, String.valueOf(code), text);
        handleException(text, cause instanceof Exception ? (Exception) cause : null, messageContext);
    }
}
