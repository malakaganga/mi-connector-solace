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

import com.google.gson.JsonElement;
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
import org.wso2.integration.connector.utils.LegacyProperties;
import org.wso2.integration.connector.utils.OperationParameters;
import org.wso2.integration.connector.utils.PayloadCodec;
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
 *
 * <p>The request body is read through the platform formatter and the reply is decoded by the payload codec
 * ({@link PayloadCodec}), by the reply's message class and content type: the {@code contentType} parameter,
 * else the type the reply declares, else the default of the class. The parameter is checked before the request
 * is published and applies to the reply only. The attributes come from the metadata helper: destination and
 * destinationType are the request's, every other attribute describes the reply.</p>
 */
public class SolaceSendRequest extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendRequest.class);
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    // Replies with another correlation id discarded in one request before the operation faults.
    private static final int MAX_MISMATCHED_REPLIES = 100;

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        // Every parameter is parsed first, so a bad value faults before any message is built or sent.
        String destinationType;
        String deliveryMode;
        String messageType;
        int timeout;
        boolean waitForAck;
        long ackTimeout;
        // The reader override applies to the reply only. The request's type comes from the message context.
        String replyContentType;
        try {
            destinationType = OperationParameters.destinationType(messageContext);
            deliveryMode = OperationParameters.deliveryMode(messageContext);
            messageType = OperationParameters.messageType(messageContext);
            // An int from the parse: the JCSMP receive takes an int and the value is never narrowed from a long.
            timeout = OperationParameters.intValue(messageContext, SolaceConstants.REQUEST_TIMEOUT,
                    DEFAULT_TIMEOUT_MS, 1, Integer.MAX_VALUE);
            // The publish acknowledgement of a guaranteed request, read as publishMessage reads them.
            waitForAck = OperationParameters.flag(messageContext, SolaceConstants.WAIT_FOR_ACK, true);
            ackTimeout = OperationParameters.ackTimeout(messageContext);
            OperationParameters.checkMessageProperties(messageContext);
            replyContentType = OperationParameters.contentType(messageContext);
        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
            return;
        }

        String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DESTINATION_NAME);
        if (StringUtils.isEmpty(destinationName)) {
            handleException("Destination name is required for sendRequest operation.", messageContext);
            return;
        }

        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)
                && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
            handleException("Invalid delivery mode 'DIRECT' for QUEUE destination '" + destinationName
                    + "'. Queues require guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                    messageContext);
            return;
        }

        // The request message is prepared here, before any broker call, so a payload fault sends nothing.
        final BytesXMLMessage request;
        try {
            PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(messageContext, "sendRequest");
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext,
                    "sendRequest");
            request = MessageBuilder.createMessage(messageType, payload, messageContext, "sendRequest");
            request.setHTTPContentType(payload.contentType());
            request.setDeliveryMode(MessageBuilder.resolveDeliveryMode(deliveryMode));
            MessageBuilder.applyMessageProperties(request, msgProperties);
        } catch (SolaceFault fault) {
            fault(fault.name, fault.code, fault.getMessage(), fault.getCause(), messageContext);
            return;
        }

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
            DeliveryMode resolvedMode = request.getDeliveryMode();

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
                // Phase C ends. The reply wait is bounded by the receive itself, within the original deadline.
                pub.finish();
                response = receiveReply(flow, expectedCorrelationId, requestDeadline, destinationName, lc);
                if (response == null) {
                    throw new JCSMPRequestTimeoutException("No reply received within " + timeout + "ms");
                }
            }

            if (response == null) {
                handleException("sendRequest returned no response from '" + destinationName + "'.",
                        messageContext);
                return;
            }

            // The reply is decoded by its message class and content type, the override, else the declared type,
            // else the class default, never by its first character.
            PayloadCodec.Received received = PayloadCodec.decode(response, replyContentType, "sendRequest");

            // The legacy solace.* properties as one set, so nothing of an earlier message survives this result.
            LegacyProperties.writeReply(messageContext, response, destinationName, deliveryMode);

            // The attributes: destination and destinationType stay the request's, every other attribute describes
            // the reply. With overwriteBody the codec puts the reply in place as the body, native binary included,
            // and the response metadata goes through the response API with overwrite false, never through the
            // framework's JSON overwrite.
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("destination", destinationName);
            attributes.put("destinationType", destinationType);
            SolaceUtils.populateMessageMetadata(response, attributes);
            JsonElement value = PayloadCodec.prepareResponse(messageContext, received, overwriteBody, attributes,
                    "sendRequest");
            handleConnectorResponse(messageContext, responseVariable, false, value, null, attributes);

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

    /**
     * Waits for the reply of a guaranteed request on its temporary queue. A reply whose correlation id is present
     * and differs from the request's is discarded with a WARN and counted, and the wait goes on within the same
     * absolute deadline. A reply without a correlation id is accepted. After the discard limit the operation
     * faults, so a stream of foreign replies cannot keep it busy. The flow is auto acknowledged, so a discarded
     * reply needs no settlement. Returns null when the deadline passes.
     */
    private BytesXMLMessage receiveReply(FlowReceiver flow, String expectedCorrelationId, long requestDeadline,
                                         String destinationName, SolaceLogicalConnection lc)
            throws JCSMPException, SolaceFault {
        int discarded = 0;
        while (true) {
            long remaining = requestDeadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return null;
            }
            // The request timeout is an int, so the remaining time fits one. Checked, never narrowed silently.
            BytesXMLMessage reply = flow.receive(Math.toIntExact(remaining));
            if (reply == null) {
                return null;
            }
            String replyCorrelationId = reply.getCorrelationId();
            if (replyCorrelationId == null || expectedCorrelationId.equals(replyCorrelationId)) {
                return reply;
            }
            discarded++;
            lc.counters.requestReplyMismatched.increment();
            log.warn("SolaceConnector [" + lc.name + "] discarded a reply with correlationId '" + replyCorrelationId
                    + "' while waiting for '" + expectedCorrelationId + "' from '" + destinationName + "'");
            if (discarded >= MAX_MISMATCHED_REPLIES) {
                throw new SolaceFault(FAULT_REQUEST_REPLY_ERROR, ERROR_CODE_REQUEST_REPLY_ERROR,
                        discarded + " replies with another correlation id were discarded while waiting for '"
                                + expectedCorrelationId + "' from '" + destinationName + "'");
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
}
