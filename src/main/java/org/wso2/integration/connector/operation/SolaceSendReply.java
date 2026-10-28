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
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.LogicalConnections;
import org.wso2.integration.connector.connection.MessageBuilder;
import org.wso2.integration.connector.connection.PollLease;
import org.wso2.integration.connector.connection.PublishOperation;
import org.wso2.integration.connector.connection.RequestReplySession;
import org.wso2.integration.connector.connection.SolaceLogicalConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.connection.TransactionBridge;
import org.wso2.integration.connector.connection.TransactionCommand;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.OperationParameters;
import org.wso2.integration.connector.utils.PayloadCodec;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_CONNECTION_MISMATCH;

/**
 * Sends a reply to an inbound request message using JCSMP's native sendReply API.
 * Requires the original inbound {@link BytesXMLMessage} to be available in the message context
 * (set by the inbound endpoint under {@link SolaceConstants#SOLACE_INBOUND_MESSAGE}).
 */
public class SolaceSendReply extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendReply.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        SolaceLogicalConnection lc = LogicalConnections.get(connectionName);
        if (lc == null) {
            fault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + connectionName + "' is not initialised", null, messageContext);
            return;
        }

        // A reply inside a transaction is a command on the transaction's thread: the inbound endpoint's
        // producer for an inbound owned one, the pinned connection's transacted producer for a connector owned one.
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        Object inboundTx = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX);
        boolean inboundOwned = inboundTx instanceof Executor;
        boolean transacted = inboundOwned || txId != null;

        PublishOperation op = null;
        try {
            // Every parameter is parsed before the request message is read, so a bad value settles nothing and
            // sends nothing. target selects the request message: INBOUND, the default, is the raw message the
            // inbound endpoint stashed, POLL is the message of the lease a poll with ackMode=CLIENT stashed.
            String target = OperationParameters.target(messageContext);
            String deliveryMode = OperationParameters.deliveryMode(messageContext);
            String messageType = OperationParameters.messageType(messageContext);
            // The publish acknowledgement of a guaranteed reply, read as publishMessage reads them.
            boolean waitForAck = OperationParameters.flag(messageContext, SolaceConstants.WAIT_FOR_ACK, true);
            long ackTimeout = OperationParameters.ackTimeout(messageContext);
            OperationParameters.checkMessageProperties(messageContext);
            BytesXMLMessage inboundMessage;
            if (SolaceConstants.TARGET_POLL.equals(target)) {
                Object stashed = messageContext.getProperty(SolaceConstants.SOLACE_POLL_MESSAGE);
                if (!(stashed instanceof PollLease)) {
                    throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                            "no polled Solace message in the message context (target=POLL); poll with ackMode=CLIENT first");
                }
                inboundMessage = ((PollLease) stashed).message;
            } else {
                // Retrieve the original inbound message for sendReply
                Object msgObj = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE);
                if (msgObj == null || !(msgObj instanceof BytesXMLMessage)) {
                    handleException("No inbound Solace message found in message context. "
                            + "Ensure this operation is used in a request-reply flow where the inbound endpoint "
                            + "sets the '" + SolaceConstants.SOLACE_INBOUND_MESSAGE + "' property.", messageContext);
                    return;
                }
                inboundMessage = (BytesXMLMessage) msgObj;
            }

            if (inboundMessage.getReplyTo() == null) {
                handleException("Inbound message has no reply-to destination. "
                        + "Cannot send reply to a message that is not part of a request-reply flow.",
                        messageContext);
                return;
            }

            // Transacted sessions only carry guaranteed messages — DIRECT bypasses the spool
            // and has no rollback semantics, so the JCSMP transacted producer rejects it.
            if (transacted && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
                String replyToName = inboundMessage.getReplyTo().getName();
                handleException("Invalid delivery mode 'DIRECT' inside a transaction (txId=" + txId
                        + ", destination='" + replyToName + "'). Transacted sessions only support"
                        + " guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                        messageContext);
                return;
            }

            // The reply body and the properties are read here, before any broker call, so a payload fault sends
            // nothing.
            PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(messageContext, "sendReply");
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext, "sendReply");

            BytesXMLMessage replyMessage = MessageBuilder.createMessage(messageType, payload, messageContext,
                    "sendReply");
            replyMessage.setHTTPContentType(payload.contentType());
            MessageBuilder.applyDeliveryMode(replyMessage, deliveryMode);

            // Echo correlationId and applicationMessageId from the request if the caller
            // didn't explicitly supply them — matches the JCSMP request-reply convention.
            if (msgProperties == null || msgProperties.getCorrelationId() == null) {
                if (inboundMessage.getCorrelationId() != null) {
                    replyMessage.setCorrelationId(inboundMessage.getCorrelationId());
                }
            }
            if (msgProperties == null || msgProperties.getApplicationMessageId() == null) {
                if (inboundMessage.getApplicationMessageId() != null) {
                    replyMessage.setApplicationMessageId(inboundMessage.getApplicationMessageId());
                }
            }

            MessageBuilder.applyMessageProperties(replyMessage, msgProperties);

            if (transacted && waitForAck && log.isDebugEnabled()) {
                log.debug("waitForAck=true is ignored for transacted publishes — atomicity is provided by solace.commit");
            }

            if (inboundOwned) {
                final XMLMessageProducer producer = (XMLMessageProducer) messageContext.getProperty(
                        SolaceConstants.SOLACE_INBOUND_TX_PRODUCER);
                TransactionBridge.submitAndWait((Executor) inboundTx, new TransactionCommand<>(() -> {
                    String correlationKey = MessageBuilder.newCorrelationKey("tx");
                    replyMessage.setCorrelationKey(correlationKey);
                    producer.sendReply(inboundMessage, replyMessage);
                    return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey, null);
                }, ackTimeout), "sendReply", FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR, messageContext);
            } else if (txId != null) {
                final SolaceTransactionRegistry.Entry entry = SolaceTransactionRegistry.get(txId);
                if (entry == null) {
                    log.error("solace.sendReply: txId=" + txId + " not found in TransactionRegistry");
                    handleException("Transaction " + txId + " not found", messageContext);
                    return;
                }
                // The reply goes through the transaction's pinned connection, so the operation's connection must
                // be the transaction's. Checked before any command is submitted, the transaction stays open.
                if (!connectionName.equals(entry.connectionName)) {
                    throw new SolaceFault(FAULT_TRANSACTION_CONNECTION_MISMATCH,
                            ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH, "operation on connection " + connectionName
                            + " inside the transaction of connection " + entry.connectionName);
                }
                TransactionBridge.submitAndWait(entry.executor, new TransactionCommand<>(() -> {
                    String correlationKey = MessageBuilder.newCorrelationKey("tx");
                    replyMessage.setCorrelationKey(correlationKey);
                    entry.connection.sendReplyTransacted(inboundMessage, replyMessage);
                    return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey, null);
                }, ackTimeout), "sendReply", FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR, messageContext);
            } else {
                // The record first, then the reply session acquired with it: a first connect attaches the candidate
                // session through the record and takes it over at publication. No pooled connection is borrowed.
                op = PublishOperation.register(lc, ackTimeout);
                RequestReplySession rr = lc.requestReplySession(op);
                op.attach(rr);
                PublishResult result = rr.publisher.send(op, replyMessage, null, inboundMessage, waitForAck);
                if (SolaceConstants.ACK_STATUS_NACK.equals(result.getAckStatus())) {
                    throw new SolaceFault(FAULT_NACK, ERROR_CODE_NACK, result.getError());
                }
            }

            // Legacy solace.* context properties — kept for callers that read them directly.
            String replyToName = inboundMessage.getReplyTo() != null
                    ? inboundMessage.getReplyTo().getName() : "unknown";
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, replyToName);
            messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);

            if (log.isDebugEnabled()) {
                log.debug("Reply sent to '" + replyToName + "'"
                        + (inboundMessage.getCorrelationId() != null
                                ? " with correlationId: " + inboundMessage.getCorrelationId() : ""));
            }

            // Build the response envelope so ${vars.X.payload.sent} resolves and
            // overwriteBody behaves consistently with other framework operations.
            JSONObject response = new JSONObject();
            response.put("sent", true);
            response.put("replyTo", replyToName);
            response.put("deliveryMode", deliveryMode);
            response.put("messageType", messageType);
            if (inboundMessage.getCorrelationId() != null) {
                response.put("correlationId", inboundMessage.getCorrelationId());
            }
            response.put("repliedAt", System.currentTimeMillis());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("sent", true);
            attributes.put("replyTo", replyToName);
            attributes.put("deliveryMode", deliveryMode);
            if (inboundMessage.getCorrelationId() != null) {
                attributes.put("correlationId", inboundMessage.getCorrelationId());
            }
            // Side-effect operation: sendReply() returns void, so this is a synthesized
            // status envelope, not broker data. Never overwrite the message body with it —
            // the original payload must keep flowing downstream. (overwriteBody is hidden
            // in the UI for this reason.)
            handleConnectorResponse(messageContext, responseVariable, false,
                    response.toString(), null, attributes);

        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
        } catch (JCSMPException e) {
            handleException("Error sending reply to Solace: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Failed to execute sendReply operation: " + e.getMessage(), e, messageContext);
        } finally {
            if (op != null) {
                op.finish();
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
