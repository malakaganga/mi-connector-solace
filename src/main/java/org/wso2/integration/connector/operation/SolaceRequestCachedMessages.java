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
import org.wso2.integration.connector.utils.SolaceUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DISABLED / NOT YET RELEASED: this operation is intentionally hidden from the
 * connector's operation palette — its component is left unregistered in
 * consume/component.xml. The class and its resource files are retained in place and
 * will be released once the operation has been tested end-to-end against a real
 * PubSub+ Cache instance.
 *
 * <p>Issues a Solace Cache request to retrieve historical messages cached for a topic.
 *
 * <p><b>Prerequisites — this operation only succeeds when the broker side is set up:</b></p>
 * <ul>
 *   <li>A PubSub+ Cache Instance is deployed and joined to the Message VPN.</li>
 *   <li>The cache instance is configured to cache the requested topic pattern (cache
 *       instances cache only topics they have been subscribed to administratively).</li>
 *   <li>The cache instance name passed here matches the broker's cache instance name.</li>
 *   <li>The connecting client has authorization to issue cache requests on the VPN.</li>
 * </ul>
 * <p>If any of these is missing, the broker returns an error and the operation fails —
 * the connector cannot detect the misconfiguration ahead of time.</p>
 *
 * <p>Cache requests are best suited to <i>state-like</i> topics (prices, last-known sensor
 * readings, configuration). Guaranteed-delivery state recovery should use queues + replay,
 * not cache.</p>
 */
public class SolaceRequestCachedMessages extends AbstractConnectorOperation {

    private static final Log log = LogFactory.getLog(SolaceRequestCachedMessages.class);

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
            String requestTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CACHE_REQUEST_TIMEOUT);
            long requestTimeout = SolaceUtils.parseLongOrDefault(requestTimeoutStr,
                    SolaceConstants.DEFAULT_CACHE_REQUEST_TIMEOUT_MS, SolaceConstants.CACHE_REQUEST_TIMEOUT);

            connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            // The first call after the borrow: connects the holder on first use, or rebuilds it (guide 3.3).
            connection.ensureSession(System.currentTimeMillis() + requestTimeout);
            if (connection == null || !connection.isConnected()) {
                handleException("Solace connection is not available or not connected.", messageContext);
                return;
            }

            String cacheName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CACHE_NAME);
            if (StringUtils.isEmpty(cacheName)) {
                handleException("cacheName is required for requestCachedMessages. This must match the"
                        + " name of a PubSub+ Cache Instance configured on the broker for this VPN.",
                        messageContext);
                return;
            }

            String topicPattern = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.TOPIC_PATTERN);
            if (StringUtils.isEmpty(topicPattern)) {
                handleException("topicPattern is required for requestCachedMessages. The cache"
                        + " instance must have been configured to cache messages on this topic.",
                        messageContext);
                return;
            }

            String maxMessagesStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CACHE_MAX_MESSAGES);
            long maxMessages = SolaceUtils.parseLongOrDefault(maxMessagesStr,
                    SolaceConstants.DEFAULT_CACHE_MAX_MESSAGES, SolaceConstants.CACHE_MAX_MESSAGES);

            String maxAgeStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CACHE_MAX_AGE_SECONDS);
            int maxAge = SolaceUtils.parseIntOrDefault(maxAgeStr, 0, SolaceConstants.CACHE_MAX_AGE_SECONDS);

            String liveDataAction = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CACHE_LIVE_DATA_ACTION);
            if (StringUtils.isEmpty(liveDataAction)) {
                liveDataAction = SolaceConstants.CACHE_LIVE_DATA_FULFILL;
            }

            // Drain window mirrors the request timeout — cached messages arrive during/after
            // the cache request returns; we keep listening at most this long.
            long drainTimeout = requestTimeout;

            if (log.isDebugEnabled()) {
                log.debug("Cache request: cacheName='" + cacheName + "', topicPattern='" + topicPattern
                        + "', maxMessages=" + maxMessages + ", maxAgeSeconds=" + maxAge
                        + ", requestTimeout=" + requestTimeout + ", liveDataAction=" + liveDataAction);
            }

            List<BytesXMLMessage> messages = connection.requestCachedMessages(
                    cacheName, topicPattern, maxMessages, maxAge, requestTimeout,
                    liveDataAction, drainTimeout);

            JSONArray messagesJson = new JSONArray();
            for (BytesXMLMessage msg : messages) {
                messagesJson.put(toSummary(msg));
            }

            // Payload = the cached messages array (so vars.X.payload is directly iterable
            // and overwriteBody=true puts the array on the body). Cache metadata goes in
            // attributes — accessible via vars.X.attributes.cacheName etc.
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("cacheName", cacheName);
            attributes.put("topicPattern", topicPattern);
            attributes.put(SolaceConstants.RESULT_MESSAGE_COUNT, messages.size());
            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    messagesJson.toString(), null, attributes);

        } catch (SolaceFault fault) {
            String text = fault.name + ": " + fault.getMessage();
            ConnectorUtils.setErrorPropertiesToMessage(messageContext, String.valueOf(fault.code), text);
            handleException(text, fault.getCause() instanceof Exception ? (Exception) fault.getCause() : null,
                    messageContext);
        } catch (JCSMPException e) {
            // Surface the prerequisites in the error so users have a hint about likely root cause.
            handleException("Solace cache request failed: " + e.getMessage()
                    + ". Verify that (1) a PubSub+ Cache Instance exists for this VPN,"
                    + " (2) it caches the requested topic pattern, and (3) the cache instance"
                    + " name matches the broker configuration.", e, messageContext);
        } catch (IllegalArgumentException e) {
            // Invalid parameter combination caught before the broker call — most commonly a
            // wildcard topic paired with a non-FLOW_THRU live data action. Surface the
            // specific reason rather than the generic "operation failed" message.
            handleException("Solace cache request rejected: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Solace cache request operation failed (connection: " + connectionName + ")",
                    e, messageContext);
        } finally {
            if (connection != null) {
                connection.returnToPool();
            }
        }
    }

    private JSONObject toSummary(BytesXMLMessage message) {
        JSONObject summary = new JSONObject();
        if (message.getMessageId() != null) {
            summary.put("messageId", message.getMessageId());
        }
        if (message.getDestination() != null) {
            summary.put("topic", message.getDestination().getName());
        }
        if (message.getApplicationMessageId() != null) {
            summary.put("applicationMessageId", message.getApplicationMessageId());
        }
        if (message.getReceiveTimestamp() != 0) {
            summary.put("receiveTimestamp", message.getReceiveTimestamp());
        }
        if (message.getSenderTimestamp() != null) {
            summary.put("senderTimestamp", message.getSenderTimestamp());
        }
        summary.put("cacheRequest", message.isCacheMessage());

        byte[] bytes = SolaceUtils.extractRawPayload(message);
        summary.put("payload", new String(bytes, StandardCharsets.UTF_8));
        String contentType = message.getHTTPContentType();
        if (StringUtils.isEmpty(contentType)) {
            contentType = SolaceUtils.inferContentType(message);
        }
        summary.put("contentType", contentType);
        return summary;
    }
}
