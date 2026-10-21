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
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.PollLease;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.settlement.HandleCompletion;
import org.wso2.integration.connector.settlement.SettlementResult;

import java.util.HashMap;
import java.util.Map;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;

/**
 * Acknowledges a message received by the inbound endpoint.
 * Should be called at the point in the mediation sequence where processing
 * is confirmed successful, so the broker can remove the message from the queue.
 *
 * Expects the inbound endpoint to have stored the original {@link BytesXMLMessage}
 * in the message context under the {@link SolaceConstants#SOLACE_INBOUND_MESSAGE} property.
 *
 * <p>In 2.0.0 the default target INBOUND completes the inbound endpoint's settlement handle when one is present,
 * and falls back to the direct settle above for inbound 1.x. The target POLL settles the message a poll with
 * ackMode=CLIENT stashed, on its flow owner thread.</p>
 */
public class SolaceAcknowledgeMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceAcknowledgeMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String target = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.TARGET);
        if (StringUtils.isEmpty(target)) {
            target = SolaceConstants.TARGET_INBOUND;
        }
        SettlementResult result;
        if (SolaceConstants.TARGET_POLL.equalsIgnoreCase(target)) {
            Object stashed = messageContext.getProperty(SolaceConstants.SOLACE_POLL_MESSAGE);
            if (!(stashed instanceof PollLease)) {
                fault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                        "no polled Solace message in the message context (target=POLL); poll with ackMode=CLIENT first",
                        null, messageContext);
                return;
            }
            PollLease lease = (PollLease) stashed;
            try {
                // Runs on the flow's owner thread. The caller waits at most OWNER_CALL_TIMEOUT_MS.
                result = lease.owner.settle(lease, XMLMessage.Outcome.ACCEPTED);
            } catch (SolaceFault fault) {
                // POLL_ERROR, UNKNOWN or a JCSMP failure: the stash is kept. A later poll consults the lease's own
                // state, OUTSTANDING blocks it and SETTLED, EXPIRED or STALE does not.
                fault(fault, messageContext, responseVariable);
                return;
            }
            // The stash is cleared whenever the operation returns a result, settled or not settled.
            messageContext.getPropertyKeySet().remove(SolaceConstants.SOLACE_POLL_MESSAGE);
        } else {
            result = HandleCompletion.completeInbound(messageContext, HandleCompletion.VALUE_ACK,
                    "acknowledgeMessage", log);
            if (result == null) {
                // Inbound 1.x: the direct settle, exactly as 1.0.1 did it.
                Object msgObj = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE);
                if (msgObj == null) {
                    handleException("No inbound Solace message found in message context, cannot acknowledge.", messageContext);
                    return;
                }

                if (!(msgObj instanceof BytesXMLMessage)) {
                    handleException("Invalid Solace message object.", messageContext);
                    return;
                }

                BytesXMLMessage solaceMessage = (BytesXMLMessage) msgObj;

                try {
                    solaceMessage.ackMessage();
                    // Tell the inbound listener the message has been settled so it doesn't issue a
                    // second ack post-mediation.
                    messageContext.setProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE_SETTLED, Boolean.TRUE);

                    if (log.isDebugEnabled()) {
                        log.debug("Message acknowledged successfully. AckMessageId: " + solaceMessage.getAckMessageId());
                    }
                    result = SettlementResult.direct(HandleCompletion.VALUE_ACK, solaceMessage.getAckMessageId());
                } catch (Exception e) {
                    handleException("Error acknowledging Solace message: " + e.getMessage(), e, messageContext);
                    return;
                }
            }
        }

        // Side-effect operation: ackMessage() returns void, so this is a synthesized
        // status envelope, not broker data. Never overwrite the message body with it —
        // the original payload must keep flowing downstream. (overwriteBody is hidden
        // in the UI for this reason.)
        handleConnectorResponse(messageContext, responseVariable, false,
                HandleCompletion.payload(result).toString(), null, HandleCompletion.attributes(result));
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
