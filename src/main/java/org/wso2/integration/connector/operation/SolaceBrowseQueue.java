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
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.utils.OperationParameters;
import org.wso2.integration.connector.utils.PayloadCodec;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Browses messages on a Solace queue without consuming them. Returns a JSON array of
 * message summaries on the response variable. The queue is left undisturbed — live
 * consumers continue to see the same messages.
 *
 * <p>Each summary carries the attributes of the metadata helper and the body as the payload codec
 * ({@link PayloadCodec}) reads it: text as it is, binary as base64 text with {@code payloadEncoding}. The
 * content type of every message is the {@code contentType} parameter, else the declared type, else the default
 * of the class. The parameter is checked before the browse starts.</p>
 */
public class SolaceBrowseQueue extends AbstractConnectorOperation {

    private static final Log log = LogFactory.getLog(SolaceBrowseQueue.class);

    // The attributes of an item whose payload the codec could not convert.
    static final String RESULT_PAYLOAD_ERROR_CODE = "payloadErrorCode";
    static final String RESULT_PAYLOAD_ERROR_MESSAGE = "payloadErrorMessage";

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        SolaceConnection connection = null;
        try {
            // Every parameter is parsed before the borrow, so a bad value never reaches the broker.
            int timeout = OperationParameters.intValue(messageContext, SolaceConstants.BROWSE_TIMEOUT,
                    SolaceConstants.DEFAULT_BROWSE_TIMEOUT_MS, 1, Integer.MAX_VALUE);
            int maxMessages = OperationParameters.intValue(messageContext, SolaceConstants.MAX_MESSAGES,
                    SolaceConstants.DEFAULT_BROWSE_MAX_MESSAGES, 1, Integer.MAX_VALUE);
            // The reader override, applied to every summarised message.
            String contentType = OperationParameters.contentType(messageContext);
            String queueName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.QUEUE_NAME);
            if (StringUtils.isEmpty(queueName)) {
                handleException("Queue name is required for browse operation.", messageContext);
                return;
            }
            String selector = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.SELECTOR);

            connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            // The first call after the borrow: connects the holder on first use, or rebuilds it.
            connection.ensureSession(System.currentTimeMillis() + timeout);
            if (connection == null || !connection.isConnected()) {
                handleException("Solace connection is not available or not connected.", messageContext);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Browsing queue '" + queueName + "' (maxMessages=" + maxMessages
                        + ", timeout=" + timeout + "ms"
                        + (selector != null ? ", selector='" + selector + "'" : "") + ")");
            }

            List<BytesXMLMessage> browsed = connection.browseQueue(queueName, maxMessages, timeout, selector);
            JSONArray messagesJson = summarise(browsed, contentType);

            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, queueName);

            // Payload = the messages array (so vars.X.payload is directly iterable, and
            // overwriteBody=true puts the array on the body). Counts/destination go in
            // attributes — accessible via vars.X.attributes.messageCount.
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(SolaceConstants.RESULT_DESTINATION, queueName);
            attributes.put(SolaceConstants.RESULT_MESSAGE_COUNT, browsed.size());

            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    messagesJson.toString(), null, attributes);

        } catch (SolaceFault fault) {
            String text = fault.name + ": " + fault.getMessage();
            ConnectorUtils.setErrorPropertiesToMessage(messageContext, String.valueOf(fault.code), text);
            handleException(text, fault.getCause() instanceof Exception ? (Exception) fault.getCause() : null,
                    messageContext);
        } catch (JCSMPException e) {
            handleException("Solace browse failed: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Solace browse operation failed (connection: " + connectionName + ")",
                    e, messageContext);
        } finally {
            if (connection != null) {
                connection.returnToPool();
            }
        }
    }

    /**
     * The summaries of the browsed messages in their order. A message whose payload cannot be converted is
     * reported on its own item and the browse continues. Nothing is acknowledged or removed.
     */
    static JSONArray summarise(List<BytesXMLMessage> browsed, String contentType) {
        JSONArray summaries = new JSONArray();
        for (BytesXMLMessage message : browsed) {
            summaries.put(toSummary(message, contentType));
        }
        return summaries;
    }

    /**
     * The summary of one message. When its payload cannot be converted, the item keeps its message id and its
     * metadata, carries the error code and the fault message, and has no payload.
     */
    static JSONObject toSummary(BytesXMLMessage message, String contentType) {
        // Pre-fill the destination from the message's Destination object (browse always
        // returns this, unlike a polled message where the queue name is the parameter)
        // so populateMessageMetadata's putIfAbsent leaves it alone.
        Map<String, Object> fields = new HashMap<>();
        if (message.getDestination() != null) {
            fields.put("destination", message.getDestination().getName());
        }
        SolaceUtils.populateMessageMetadata(message, fields);

        PayloadCodec.Received received;
        try {
            received = PayloadCodec.decode(message, contentType, "browse");
        } catch (SolaceFault fault) {
            fields.put(RESULT_PAYLOAD_ERROR_CODE, fault.code);
            fields.put(RESULT_PAYLOAD_ERROR_MESSAGE, fault.getMessage());
            return new JSONObject(fields);
        }
        fields.put(SolaceConstants.RESULT_CONTENT_TYPE, received.contentType);
        if (received.isBinary()) {
            fields.put(SolaceConstants.RESULT_PAYLOAD_ENCODING, SolaceConstants.PAYLOAD_ENCODING_BASE64);
        }
        JSONObject summary = new JSONObject(fields);
        summary.put("payload", received.summaryText());
        return summary;
    }
}
