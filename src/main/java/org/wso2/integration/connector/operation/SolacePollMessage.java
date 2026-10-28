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
import com.solacesystems.jcsmp.JCSMPException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.LogicalConnections;
import org.wso2.integration.connector.connection.PollAdmission;
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
import org.wso2.integration.connector.utils.LegacyProperties;
import org.wso2.integration.connector.utils.OperationParameters;
import org.wso2.integration.connector.utils.PayloadCodec;
import org.wso2.integration.connector.utils.SolaceUtils;

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
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_UNSETTLED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_COMPLETED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_CONNECTION_MISMATCH;
import static org.wso2.integration.connector.constants.SolaceConstants.TX_POLL_OWNER_ALLOWANCE_MS;

/**
 * Synchronously polls one message from a Solace queue. Returns null on timeout.
 * The polled message's payload becomes the response variable's payload (or replaces
 * the message body if {@code overwriteBody} is true). Metadata such as messageId,
 * correlationId, and the received/timedOut flags are exposed as response attributes
 * (accessible via {@code ${vars.X.attributes.messageId}}) and as legacy
 * {@code solace.*} message-context properties.
 *
 * <p>The polled message is decoded by the payload codec ({@link PayloadCodec}), by its message class and
 * content type: the {@code contentType} parameter, else the declared type, else the default of the class. A
 * JSON family body is the parsed value, other text a JSON string, binary a base64 string with
 * {@code attributes.payloadEncoding}. A {@code contentType} that is not a media type, or names a charset Java
 * does not know, faults INVALID_CONFIG before any broker call. A body the codec cannot convert faults
 * PAYLOAD_ERROR after the message was received, so with ackMode=AUTO it is already acknowledged and with
 * CLIENT the lease stays for the caller to settle.</p>
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

        // An int from the parse: the JCSMP receive takes an int and the value is never narrowed from a long.
        int timeout;
        // The reader override, checked here so a bad value faults before any broker call.
        String contentType;
        try {
            timeout = OperationParameters.intValue(messageContext, SolaceConstants.POLL_TIMEOUT,
                    SolaceConstants.DEFAULT_POLL_TIMEOUT_MS, 1, Integer.MAX_VALUE);
            contentType = OperationParameters.contentType(messageContext);
        } catch (SolaceFault fault) {
            fault(fault, messageContext, responseVariable);
            return;
        }

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
            log.debug("solace.poll: txId=" + txId
                    + " resolved to connectionId=" + entry.connection.getConnectionId());
            // The poll runs on the transaction's pinned connection, so the operation's connection must be the
            // transaction's. Checked before any command is submitted, the transaction stays open.
            if (!connectionName.equals(entry.connectionName)) {
                fault(FAULT_TRANSACTION_CONNECTION_MISMATCH, ERROR_CODE_TRANSACTION_CONNECTION_MISMATCH,
                        "operation on connection " + connectionName + " inside the transaction of connection "
                                + entry.connectionName, null, messageContext);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Polling queue '" + queueName + "' (timeout=" + timeout + "ms"
                        + (selector != null ? ", selector='" + selector + "'" : "")
                        + ", transactional=" + isTransactional + ")");
            }
            final String pollSelector = selector;
            // One outstanding consuming poll per transaction, on any queue, reserved before the command is
            // submitted. A poll that finds the reservation taken faults at once and never waits.
            final PollAdmission.Token token;
            try {
                token = entry.pollAdmission.reserve();
            } catch (SolaceFault fault) {
                fault(fault, messageContext, responseVariable);
                return;
            }
            TransactionCommand<BytesXMLMessage> command = null;
            try {
                // The owner's budget is the poll timeout plus its allowance. Checked arithmetic, because the largest
                // poll timeout plus the allowance does not fit an int. The receive itself gets the poll timeout.
                long budget = Math.addExact((long) timeout, TX_POLL_OWNER_ALLOWANCE_MS);
                Hooks.get().point("tx.pollSubmitted", entry);
                command = new TransactionCommand<>(() -> {
                    Hooks.get().point("tx.pollExecuted", entry);
                    BytesXMLMessage received = entry.connection.pollTransacted(queueName, timeout,
                            pollSelector);
                    if (received != null) {
                        // On the owner, before the result is exposed. The reservation then stays for the
                        // life of the transaction.
                        entry.pollAdmission.recordConsumption();
                    }
                    return received;
                }, budget, "tx.pollAfterClaim");
                BytesXMLMessage message = TransactionBridge.submitAndWait(entry.executor, command, "poll",
                        FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, messageContext);
                if (message == null) {
                    // The owner confirmed an empty receive.
                    entry.pollAdmission.release(token);
                }
                writeResult(messageContext, message, queueName, responseVariable, overwriteBody, null, null,
                        contentType);
            } catch (SolaceFault fault) {
                // Released only on proof that the command never ran. That is a command cancelled while still
                // queued, the DEADLINE path, or a submission the executor refused, which leaves the command
                // untouched. A caller timeout on a running command is UNKNOWN and keeps the reservation, and so
                // does every other fault.
                if (FAULT_DEADLINE.equals(fault.name) || (FAULT_TRANSACTION_COMPLETED.equals(fault.name)
                        && command != null && !command.isCancelled())) {
                    entry.pollAdmission.release(token);
                }
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
            // The measurement from the request to its result.
            Hooks.get().record("poll.requestToResult", System.nanoTime() - request.createdNanos);
            if (lease != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_POLL_MESSAGE, lease);
            }
            writeResult(messageContext, message, queueName, responseVariable, overwriteBody, ackMode, lease,
                    contentType);
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
     * Builds the response attributes and the payload and routes them through
     * {@link #handleConnectorResponse} so {@code ${vars.X}} resolves correctly. The payload codec decides the
     * representation and, with {@code overwriteBody}, puts the body in place itself.
     */
    private void writeResult(MessageContext messageContext, BytesXMLMessage message, String queueName,
                             String responseVariable, Boolean overwriteBody, String ackMode, PollLease lease,
                             String contentType) throws SolaceFault {

        // The legacy solace.* properties as one set. A timed out poll removes them.
        LegacyProperties.writePolled(messageContext, message, queueName);

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

        if (message == null) {
            // Timeout path. Use a JSON envelope so callers can branch on receivedflag
            // without dereferencing a null payload.
            JSONObject envelope = new JSONObject();
            envelope.put(SolaceConstants.RESULT_RECEIVED, false);
            envelope.put(SolaceConstants.RESULT_TIMED_OUT, true);
            envelope.put(SolaceConstants.RESULT_DESTINATION, queueName);
            handleConnectorResponse(messageContext, responseVariable, overwriteBody, envelope.toString(),
                    null, attributes);
            return;
        }

        // Standard SMF headers and the publisher's user properties, as ${vars.X.attributes.*}.
        SolaceUtils.populateMessageMetadata(message, attributes);

        // The codec decides the representation by the message class and the content type, the override, else the
        // declared type, else the class default. With overwriteBody the codec itself puts the body in place,
        // native binary included, and the response metadata goes through the response API with overwrite false,
        // never through the framework's JSON overwrite, which would replace the native body.
        PayloadCodec.Received received = PayloadCodec.decode(message, contentType, "poll");
        JsonElement value = PayloadCodec.prepareResponse(messageContext, received, overwriteBody, attributes,
                "poll");
        handleConnectorResponse(messageContext, responseVariable, false, value, null, attributes);
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
