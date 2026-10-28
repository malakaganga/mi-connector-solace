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
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.LogicalConnections;
import org.wso2.integration.connector.connection.MessageBuilder;
import org.wso2.integration.connector.connection.PublishOperation;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.SolaceLogicalConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.connection.TransactionBridge;
import org.wso2.integration.connector.connection.TransactionCommand;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
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
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_EXPIRED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_NACK;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_CONNECTION_MISMATCH;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;

/**
 * Publishes a message to Solace (operation class).
 */
public class SolacePublishMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolacePublishMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        boolean isTransactional = (txId != null);
        if (isTransactional) {
            log.debug("solace.publishMessage: transactional path, txId=" + txId
                    + ", connectionName=" + connectionName);
        }

        SolaceTransactionRegistry.Entry transactionEntry = null;
        if (isTransactional) {
            transactionEntry = SolaceTransactionRegistry.get(txId);
            if (transactionEntry == null) {
                log.error("solace.publishMessage: txId=" + txId + " not found in TransactionRegistry");
                handleException("Transaction " + txId + " not found", messageContext);
                return;
            }
            log.debug("solace.publishMessage: txId=" + txId
                    + " resolved to connectionId=" + transactionEntry.connection.getConnectionId());
            // The publish goes through the transaction's pinned connection, so the operation's connection must be
            // the transaction's. Checked before any command is submitted, the transaction stays open.
            if (!connectionName.equals(transactionEntry.connectionName)) {
                fault(FAULT_TRANSACTION_CONNECTION_MISMATCH, ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH,
                        "operation on connection " + connectionName + " inside the transaction of connection "
                                + transactionEntry.connectionName, null, messageContext);
                return;
            }
        }
        // An inbound owned transaction: the inbound endpoint's worker owns the executor and the producer.
        Object inboundTx = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX);
        boolean inboundOwned = inboundTx instanceof Executor;

        // Every parameter is parsed first, so a bad value faults before any message is built or sent.
        String destinationType;
        String deliveryMode;
        String messageType;
        boolean waitForAck;
        long ackTimeout;
        boolean continueOnAckFailure;
        try {
            destinationType = OperationParameters.destinationType(messageContext);
            deliveryMode = OperationParameters.deliveryMode(messageContext);
            messageType = OperationParameters.messageType(messageContext);
            // Publish ACK wait (guaranteed mode only). The default is true for guaranteed delivery.
            waitForAck = OperationParameters.flag(messageContext, SolaceConstants.WAIT_FOR_ACK, true);
            ackTimeout = OperationParameters.ackTimeout(messageContext);
            continueOnAckFailure = OperationParameters.flag(messageContext, SolaceConstants.CONTINUE_ON_ACK_FAILURE,
                    false);
            OperationParameters.checkMessageProperties(messageContext);
        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
            return;
        }
        String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DESTINATION_NAME);
        if (StringUtils.isEmpty(destinationName)) {
            handleException("Destination name is required for publishing a message.", messageContext);
            return;
        }

        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)
                && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
            handleException("Invalid delivery mode 'DIRECT' for QUEUE destination '" + destinationName
                    + "'. Queues require guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                    messageContext);
            return;
        }

        // Transacted sessions only carry guaranteed messages — DIRECT bypasses the spool
        // and has no rollback semantics, so the JCSMP transacted producer rejects it.
        if ((isTransactional || inboundOwned)
                && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
            handleException("Invalid delivery mode 'DIRECT' inside a transaction (txId=" + txId
                    + ", destination='" + destinationName + "'). Transacted sessions only support"
                    + " guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                    messageContext);
            return;
        }

        // The body, the properties and the message are prepared here, before any broker call, so a payload
        // fault sends nothing on every path.
        final BytesXMLMessage message;
        final int payloadLength;
        String httpContentType;
        try {
            PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(messageContext, "publishMessage");
            httpContentType = payload.contentType();
            payloadLength = payload.length();
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext,
                    "publishMessage");
            message = MessageBuilder.createMessage(messageType, payload, messageContext, "publishMessage");
            message.setHTTPContentType(httpContentType);
            MessageBuilder.applyDeliveryMode(message, deliveryMode);
            MessageBuilder.applyMessageProperties(message, msgProperties);
        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
            return;
        } catch (RuntimeException e) {
            // Any other failure while the message is built is a publish failure, as in 2.0.0. Nothing is sent.
            fault(FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR,
                    "Solace publish operation failed (connection: " + connectionName + ")", e, messageContext);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Publishing message to Solace with parameters: destinationType=" + destinationType
                    + ", destinationName=" + destinationName + ", deliveryMode=" + deliveryMode
                    + ", messageType=" + messageType + ", httpContentType=" + httpContentType);
        }

        if ((isTransactional || inboundOwned) && waitForAck && log.isDebugEnabled()) {
            log.debug("waitForAck=true is ignored for transacted publishes — atomicity is provided by solace.commit");
        }

        // The command bodies run on the transaction's thread, so they capture the resolved values.
        final String publishDestinationType = destinationType;

        if (isTransactional) {
            // Connector owned: the 1.0.1 transacted publish, now always on the transaction's executor thread.
            final SolaceTransactionRegistry.Entry entry = transactionEntry;
            try {
                log.debug("solace.publishMessage: txId=" + txId + " sending transacted message to "
                        + destinationType + " '" + destinationName + "' (deliveryMode=" + deliveryMode
                        + ", messageType=" + messageType + ")");
                PublishResult result = TransactionBridge.submitAndWait(entry.executor,
                        new TransactionCommand<>(() -> entry.connection.publishTransacted(publishDestinationType,
                                destinationName, message), ackTimeout),
                        "publishMessage", FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR, messageContext);
                log.debug("solace.publishMessage: txId=" + txId + " transacted send queued (correlationKey="
                        + result.getCorrelationKey() + ", ackStatus=" + result.getAckStatus() + ")");
                if (log.isDebugEnabled()) {
                    log.debug("Message published to " + destinationType + " '" + destinationName
                            + "' (deliveryMode=" + deliveryMode + ", payloadLength="
                            + payloadLength
                            + ", ackStatus=" + result.getAckStatus() + ")");
                }
                setResultInContext(messageContext, result, destinationType, destinationName, deliveryMode,
                        messageType, responseVariable, false);
            } catch (SolaceFault fault) {
                fault(fault, messageContext, responseVariable);
            }
            return;
        }

        if (inboundOwned) {
            // Inbound owned: the publish is a command on the inbound endpoint's producer. The result is the
            // commit's, so the ack status is TX_PENDING.
            final XMLMessageProducer producer = (XMLMessageProducer) messageContext.getProperty(
                    SolaceConstants.SOLACE_INBOUND_TX_PRODUCER);
            try {
                PublishResult result = TransactionBridge.submitAndWait((Executor) inboundTx,
                        new TransactionCommand<>(() -> {
                            Destination destination = MessageBuilder.resolveDestination(publishDestinationType,
                                    destinationName);
                            String correlationKey = MessageBuilder.newCorrelationKey("tx");
                            message.setCorrelationKey(correlationKey);
                            producer.send(message, destination);
                            return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey,
                                    null);
                        }, ackTimeout),
                        "publishMessage", FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR, messageContext);
                setResultInContext(messageContext, result, destinationType, destinationName, deliveryMode,
                        messageType, responseVariable, false);
            } catch (SolaceFault fault) {
                fault(fault, messageContext, responseVariable);
            }
            return;
        }

        SolaceLogicalConnection lc = LogicalConnections.get(connectionName);
        if (lc == null) {
            fault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection '" + connectionName + "' is not initialised", null, messageContext);
            return;
        }
        PublishOperation op = null;
        SolaceConnection connection = null;
        try {
            // The first statement of the try, before the borrow. From here every exit goes through op.finish().
            op = PublishOperation.register(lc, ackTimeout);
            Destination destination = MessageBuilder.resolveDestination(destinationType, destinationName);
            // Bounded by the pool's maxWaitTime.
            connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (!op.attach(connection)) {
                // A refused session was never used.
                throw new SolaceFault(FAULT_EXPIRED, ERROR_CODE_EXPIRED,
                        "deadline passed while waiting for a pooled session", null, false);
            }
            // Connects the holder on first use, or rebuilds it. Bounded by the record's deadline.
            connection.ensureSession(op);
            PublishResult result = connection.publisher.send(op, message, destination, null, waitForAck);

            if (log.isDebugEnabled()) {
                log.debug("Message published to " + destinationType + " '" + destinationName
                        + "' (deliveryMode=" + deliveryMode + ", payloadLength="
                        + payloadLength
                        + ", ackStatus=" + result.getAckStatus() + ")");
            }

            setResultInContext(messageContext, result, destinationType, destinationName, deliveryMode,
                    messageType, responseVariable, false);

            if (!result.isAckReceived() && waitForAck && !continueOnAckFailure
                    && SolaceConstants.ACK_STATUS_NACK.equals(result.getAckStatus())) {
                throw new SolaceFault(FAULT_NACK, ERROR_CODE_NACK, result.getError());
            }
        } catch (SolaceFault fault) {
            if (continueOnAckFailure && (FAULT_DEADLINE.equals(fault.name) || FAULT_UNKNOWN.equals(fault.name)
                    || FAULT_EXPIRED.equals(fault.name))) {
                setResultInContext(messageContext,
                        new PublishResult(SolaceConstants.ACK_STATUS_TIMEOUT, false, null, fault.getMessage()),
                        destinationType, destinationName, deliveryMode, messageType, responseVariable, true);
            } else {
                fault(fault, messageContext, responseVariable);
            }
        } catch (ConnectException e) {
            // The pool was exhausted within maxWaitTime, or the borrow was refused by a closed logical connection.
            fault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "Solace connection is not available: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            fault(FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR,
                    "Solace publish operation failed (connection: " + connectionName + ")", e, messageContext);
        } finally {
            // Terminal under the record's monitor before the pool return, so a running timer callback finds FAILED.
            if (op != null) {
                op.finish();
            }
            // An expired operation discards the session it had attached. A never attached session goes back healthy.
            if (connection != null) {
                if (op.expiredWith(connection.session)) {
                    connection.failed = true;
                }
                connection.returnToPool();
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

    private void setResultInContext(MessageContext messageContext, PublishResult result,
                                    String destinationType, String destinationName, String deliveryMode,
                                    String messageType, String responseVariable, boolean outcomeUnknown) {
        // Legacy solace.* context properties — kept for callers that read them directly.
        messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
        messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_STATUS, result.getAckStatus());
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_RECEIVED, String.valueOf(result.isAckReceived()));
        if (result.getCorrelationKey() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_CORRELATION_KEY, result.getCorrelationKey());
        }
        if (result.getError() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_ERROR, result.getError());
        }

        // Set the publish-result fields.
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("destination", destinationName);
        attributes.put("destinationType", destinationType);
        attributes.put("deliveryMode", deliveryMode);
        attributes.put("ackStatus", result.getAckStatus());
        attributes.put("ackReceived", result.isAckReceived());
        // correlationKey is only set for guaranteed sends and error only on failure.
        if (result.getCorrelationKey() != null) {
            attributes.put("correlationKey", result.getCorrelationKey());
        }
        if (result.getError() != null) {
            attributes.put("error", result.getError());
        }
        // The deadline cut the operation off after send(): the message may or may not have reached the broker.
        if (outcomeUnknown) {
            attributes.put("outcome", SolaceConstants.OUTCOME_UNKNOWN);
        }

        JSONObject response = new JSONObject(attributes);
        response.put("messageType", messageType);
        response.put("publishedAt", System.currentTimeMillis());

        // Side-effect operation: a Solace publish returns no payload, only this synthesized
        // result envelope (ack status etc.). Never overwrite the message body with it — the
        // original payload must keep flowing downstream. (overwriteBody is hidden in the UI
        // for this reason.)
        handleConnectorResponse(messageContext, responseVariable, false,
                response.toString(), null, attributes);
    }
}
