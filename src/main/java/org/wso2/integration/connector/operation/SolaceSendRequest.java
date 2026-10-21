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
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.LogicalConnections;
import org.wso2.integration.connector.connection.MessageBuilder;
import org.wso2.integration.connector.connection.PublishOperation;
import org.wso2.integration.connector.connection.RequestReplySession;
import org.wso2.integration.connector.connection.SolaceLogicalConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_REQUEST_CONCURRENCY_EXCEEDED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_REQUEST_IN_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_REQUEST_REPLY_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_REQUEST_CONCURRENCY_EXCEEDED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_REQUEST_IN_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_REQUEST_REPLY_ERROR;

/**
 * Sends a request message and blocks until a reply arrives or the timeout expires.
 * DIRECT delivery uses the JCSMP {@code Requestor}; PERSISTENT/NON_PERSISTENT delivery
 * uses a temporary queue with a {@code FlowReceiver} for guaranteed request-reply.
 */
public class SolaceSendRequest extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendRequest.class);
    private static final long DEFAULT_TIMEOUT_MS = 30000L;

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        // Extract parameters
        String destinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DESTINATION_TYPE);
        if (StringUtils.isEmpty(destinationType)) {
            destinationType = SolaceConstants.DESTINATION_TYPE_TOPIC;
        }

        String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DESTINATION_NAME);
        if (StringUtils.isEmpty(destinationName)) {
            handleException("Destination name is required for sendRequest operation.", messageContext);
            return;
        }

        String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DELIVERY_MODE);
        if (StringUtils.isEmpty(deliveryMode)) {
            deliveryMode = SolaceConstants.DELIVERY_MODE_DIRECT;
        }

        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)
                && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
            handleException("Invalid delivery mode 'DIRECT' for QUEUE destination '" + destinationName
                    + "'. Queues require guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                    messageContext);
            return;
        }

        String messageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.MESSAGE_TYPE);
        if (StringUtils.isEmpty(messageType)) {
            messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
        }
        messageType = messageType.toUpperCase();
        // Solace has no JSON message type — JSON payloads are carried as TEXT on the wire.
        if (!SolaceConstants.MESSAGE_TYPE_TEXT.equals(messageType)
                && !SolaceConstants.MESSAGE_TYPE_BYTES.equals(messageType)
                && !SolaceConstants.MESSAGE_TYPE_XML.equals(messageType)) {
            handleException("Unsupported messageType '" + messageType
                    + "'. Supported types: TEXT, BYTES, XML.", messageContext);
            return;
        }

        String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.REQUEST_TIMEOUT);
        long timeout = SolaceUtils.parseLongOrDefault(timeoutStr, DEFAULT_TIMEOUT_MS,
                SolaceConstants.REQUEST_TIMEOUT);

        // Get message payload and detect its content type for downstream subscribers
        String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
        String payload = payloadAndType[0];
        String httpContentType = payloadAndType[1];

        // Build optional message properties
        SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

        // The publish acknowledgement of a guaranteed request, read as publishMessage reads them.
        String waitForAckStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.WAIT_FOR_ACK);
        boolean waitForAck = StringUtils.isEmpty(waitForAckStr) ? true : Boolean.parseBoolean(waitForAckStr);
        String ackTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.ACK_TIMEOUT);
        long ackTimeout = SolaceUtils.parseLongOrDefault(ackTimeoutStr,
                SolaceConstants.DEFAULT_ACK_TIMEOUT_MS, SolaceConstants.ACK_TIMEOUT);

        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX) instanceof Executor || txId != null) {
            fault(FAULT_REQUEST_IN_TRANSACTION, ERROR_CODE_REQUEST_IN_TRANSACTION,
                    "sendRequest is not supported inside a transaction: a transacted publish is invisible to the responder until the commit",
                    null, messageContext);
            return;
        }
        SolaceLogicalConnection lc = LogicalConnections.get(connectionName);
        if (lc == null) {
            fault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + connectionName + "' is not initialised", null, messageContext);
            return;
        }

        // One clock for every phase.
        final long startedAt = System.currentTimeMillis();
        final long requestDeadline = startedAt + timeout;
        PublishOperation setup = null;
        PublishOperation pub = null;
        RequestReplySession rr = null;
        Queue tempQueue = null;
        FlowReceiver flow = null;
        boolean permitHeld = false;
        try {
            // Phase A record, deadline = the request budget. It governs the reply session's first connect only.
            // The session is transferred out of the record at publication, so it never closes the
            // shared reply session afterwards.
            setup = PublishOperation.registerUntil(lc, requestDeadline);
            rr = lc.requestReplySession(setup);
            long remaining = requestDeadline - System.currentTimeMillis();
            if (remaining <= 0) {
                // The first connect used the budget up: a timeout, not a concurrency fault.
                throw new JCSMPRequestTimeoutException("request budget spent before the permit wait");
            }
            // The permit wait is bounded by the request budget. Nothing was sent.
            if (!rr.permits.tryAcquire(remaining, TimeUnit.MILLISECONDS)) {
                throw new SolaceFault(FAULT_REQUEST_CONCURRENCY_EXCEEDED, ERROR_CODE_REQUEST_CONCURRENCY_EXCEEDED,
                        "maxConcurrentRequests=" + lc.settings.maxConcurrentRequests
                                + " requests are in flight on connection '" + lc.name + "'");
            }
            permitHeld = true;
            // Rechecked immediately after admission: an expired record or a spent budget never starts the setup.
            if (setup.isExpired() || requestDeadline - System.currentTimeMillis() <= 0) {
                throw new JCSMPRequestTimeoutException("request budget spent while waiting for a permit");
            }

            Destination destination = MessageBuilder.resolveDestination(destinationType, destinationName);
            DeliveryMode resolvedMode = MessageBuilder.resolveDeliveryMode(deliveryMode);
            BytesXMLMessage request = MessageBuilder.createMessage(messageType, payload, messageContext);
            if (httpContentType != null && !httpContentType.isEmpty()) {
                request.setHTTPContentType(httpContentType);
            }
            request.setDeliveryMode(resolvedMode);
            MessageBuilder.applyMessageProperties(request, msgProperties);

            BytesXMLMessage response;
            if (resolvedMode == DeliveryMode.DIRECT) {
                // The Requestor needs the session consumer started. Bounded by the session's readTimeout.
                rr.ensureConsumer();
                // Phase A ends before the reply wait. The Requestor bounds a Direct reply wait itself.
                setup.finish();
                remaining = requestDeadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new JCSMPRequestTimeoutException("request budget spent before the request was sent");
                }
                // Temporary topic, no queue, as 1.0.1.
                response = rr.session.createRequestor().request(request, remaining, destination);
            } else {
                // Phase B setup on the shared session. Each call is bounded by the session's readTimeout and the
                // request budget is rechecked after each call.
                JCSMPSession replySession = rr.session;
                tempQueue = replySession.createTemporaryQueue();
                if (requestDeadline - System.currentTimeMillis() <= 0) {
                    // No new bind begins on a spent budget.
                    throw new JCSMPRequestTimeoutException("request budget spent creating the reply queue");
                }
                request.setReplyTo(tempQueue);

                // Ensure the request carries a correlation ID we can verify against the reply.
                // If the caller already set one via properties, use it; otherwise generate one.
                String expectedCorrelationId = request.getCorrelationId();
                if (expectedCorrelationId == null) {
                    expectedCorrelationId = "req-" + UUID.randomUUID();
                    request.setCorrelationId(expectedCorrelationId);
                }

                ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
                flowProperties.setEndpoint(tempQueue);
                flowProperties.setStartState(true);
                // Baseline 415. The flow close in the finally removes the temporary queue.
                flow = replySession.createFlow(null, flowProperties);
                // Phase A ends. The setup never leaves a timer on the shared session.
                setup.finish();
                remaining = requestDeadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    // A spent budget never becomes permission to start a publish.
                    throw new JCSMPRequestTimeoutException("request budget spent during the reply queue setup");
                }
                // Phase C record: the guaranteed publish and its acknowledgement, attached to the shared reply
                // session. An expiry here closes it, because a publish stuck on that session blocks every request.
                pub = PublishOperation.registerUntil(lc,
                        Math.min(System.currentTimeMillis() + ackTimeout, requestDeadline));
                if (!pub.attach(rr)) {
                    throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                            "deadline passed before the request was published", null, false);
                }
                PublishResult publishResult = rr.publisher.send(pub, request, destination, null, waitForAck);
                if (SolaceConstants.ACK_STATUS_NACK.equals(publishResult.getAckStatus())) {
                    throw new SolaceFault(FAULT_NACK, ERROR_CODE_NACK,
                            "the broker rejected the request: " + publishResult.getError());
                }
                // Phase C ends. The reply wait is bounded by the receive itself.
                pub.finish();
                remaining = requestDeadline - System.currentTimeMillis();
                response = remaining > 0 ? flow.receive((int) remaining) : null;
                if (response == null) {
                    throw new JCSMPRequestTimeoutException("No reply received within " + timeout + "ms");
                }

                // Verify the reply corresponds to our request. A mismatch is unlikely on a fresh
                // temp queue, but defensive logging makes cross-wiring easier to diagnose.
                String replyCorrelationId = response.getCorrelationId();
                if (replyCorrelationId != null && !expectedCorrelationId.equals(replyCorrelationId)) {
                    log.warn("Guaranteed request-reply correlation mismatch: expected='"
                            + expectedCorrelationId + "', received='" + replyCorrelationId
                            + "'. Returning the message regardless.");
                }
            }

            if (response == null) {
                handleException("sendRequest returned no response from '" + destinationName + "'.",
                        messageContext);
                return;
            }

            // Extract raw payload bytes from response
            byte[] responseBytes = SolaceUtils.extractRawPayload(response);
            String contentType = response.getHTTPContentType();
            if (contentType == null || contentType.isEmpty()) {
                contentType = SolaceUtils.inferContentType(response);
            }

            // Legacy solace.* context properties — kept for callers that read them directly.
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
            messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
            if (response.getCorrelationId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_CORRELATION_ID, response.getCorrelationId());
            }
            if (response.getSenderId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_SENDER_ID, response.getSenderId());
            }
            if (response.getApplicationMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID,
                        response.getApplicationMessageId());
            }
            if (response.getApplicationMessageType() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                        response.getApplicationMessageType());
            }
            if (response.getReceiveTimestamp() > 0) {
                messageContext.setProperty(SolaceConstants.SOLACE_REPLY_RECEIVE_TIMESTAMP,
                        response.getReceiveTimestamp());
            }

            // Build attributes (the metadata) and the payload (the reply content) and route
            // both through handleConnectorResponse so ${vars.X.payload} works and the
            // overwriteBody flag is honored consistently with other framework operations.
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("destination", destinationName);
            attributes.put("destinationType", destinationType);
            attributes.put("deliveryMode", deliveryMode);
            attributes.put("contentType", contentType);
            if (response.getCorrelationId() != null) {
                attributes.put("correlationId", response.getCorrelationId());
            }
            if (response.getApplicationMessageId() != null) {
                attributes.put("applicationMessageId", response.getApplicationMessageId());
            }
            if (response.getApplicationMessageType() != null) {
                attributes.put("applicationMessageType", response.getApplicationMessageType());
            }
            if (response.getReceiveTimestamp() > 0) {
                attributes.put("receiveTimestamp", response.getReceiveTimestamp());
            }

            String body = new String(responseBytes, StandardCharsets.UTF_8);
            String payloadJson = SolaceUtils.looksLikeJson(body) ? body : JSONObject.quote(body);

            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    payloadJson, null, attributes);

            if (log.isDebugEnabled()) {
                log.debug("Response received successfully for request to " + destinationName);
            }

        } catch (SolaceFault fault) {
            // Counted before the fault is raised, because handleException throws.
            lc.counters.requestsFaulted.increment();
            fault(fault, messageContext, responseVariable);
        } catch (JCSMPRequestTimeoutException e) {
            fault(FAULT_REQUEST_REPLY_ERROR, ERROR_CODE_REQUEST_REPLY_ERROR,
                    "sendRequest timed out after " + timeout + "ms. No response received from '" + destinationName + "'.",
                    e, messageContext);
        } catch (InterruptedException e) {
            // Only the permit wait throws it. Nothing was sent.
            Thread.currentThread().interrupt();
            fault(FAULT_REQUEST_REPLY_ERROR, ERROR_CODE_REQUEST_REPLY_ERROR,
                    "sendRequest interrupted before the request was sent", e, messageContext);
        } catch (JCSMPException e) {
            fault(FAULT_REQUEST_REPLY_ERROR, ERROR_CODE_REQUEST_REPLY_ERROR,
                    "Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } finally {
            // Every exit, a failed bind, a NACK, a timeout and a receive error included.
            if (setup != null) {
                setup.finish();
            }
            if (pub != null) {
                pub.finish();
            }
            boolean cleanupFailed = false;
            if (flow != null) {
                try {
                    flow.close();
                } catch (Exception e) {
                    cleanupFailed = true;
                    log.warn("SolaceConnector [" + lc.name + "] could not close the reply flow: " + e.getMessage());
                }
            }
            // The flow close is the whole cleanup. The broker removes a temporary queue when its flow closes, and
            // JCSMP refuses deprovision on a temporary queue.
            if (cleanupFailed) {
                // Marks the request reply session for rebuild at its next use.
                lc.counters.cleanupFailures.increment();
                rr.failed = true;
            }
            if (permitHeld) {
                rr.permits.release();
            }
        }
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

    // Payload extraction / content-type inference moved to SolaceUtils (shared with the
    // poll, browse, and requestCachedMessages operations).
}
