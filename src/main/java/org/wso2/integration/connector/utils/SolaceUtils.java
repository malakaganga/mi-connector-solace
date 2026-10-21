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

package org.wso2.integration.connector.utils;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.axiom.om.OMElement;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.SolaceMessageProperties;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building JCSMP properties from the Synapse MessageContext.
 */
public final class SolaceUtils {

    private static final Log log = LogFactory.getLog(SolaceUtils.class);

    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final String CONTENT_TYPE_XML  = "application/xml";
    public static final String CONTENT_TYPE_TEXT = "text/plain";

    private SolaceUtils() {
        // Prevent instantiation
    }

    // ---- Safe numeric parsing -----------------------------------------------------------
    // User-supplied parameters arrive as strings. A malformed value must not crash the whole
    // operation with an uncaught NumberFormatException — instead we log a WARN and fall back
    // to a sensible value (a caller-provided default, or skipping an optional setting).

    /**
     * Parses a long, returning {@code defaultValue} when {@code value} is empty, and returning
     * {@code defaultValue} (with a WARN) when {@code value} is not a valid number.
     */
    public static long parseLongOrDefault(String value, long defaultValue, String paramName) {
        Long parsed = tryParseLong(value, paramName);
        return parsed != null ? parsed : defaultValue;
    }

    /**
     * Parses an int, returning {@code defaultValue} when {@code value} is empty, and returning
     * {@code defaultValue} (with a WARN) when {@code value} is not a valid number.
     */
    public static int parseIntOrDefault(String value, int defaultValue, String paramName) {
        Integer parsed = tryParseInt(value, paramName);
        return parsed != null ? parsed : defaultValue;
    }

    /**
     * Parses a long, or returns {@code null} (with a WARN on malformed input) so the caller can
     * skip an optional setting rather than fail.
     */
    private static Long tryParseLong(String value, String paramName) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Ignoring invalid numeric value '" + value + "' for parameter '" + paramName + "'.");
            return null;
        }
    }

    /**
     * Parses an int, or returns {@code null} (with a WARN on malformed input) so the caller can
     * skip an optional setting rather than fail.
     */
    private static Integer tryParseInt(String value, String paramName) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Ignoring invalid numeric value '" + value + "' for parameter '" + paramName + "'.");
            return null;
        }
    }

    // ---- Message payload / content-type helpers -----------------------------------------
    // Shared by the poll, browse, sendRequest, and requestCachedMessages operations so the
    // JCSMP message-type handling lives in one place.

    /**
     * Extracts the raw payload bytes from a JCSMP message, handling the Text, XMLContent, and
     * Bytes message types with a binary-attachment fallback. Never returns null.
     */
    public static byte[] extractRawPayload(BytesXMLMessage message) {
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            return text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        } else if (message instanceof XMLContentMessage) {
            String xml = ((XMLContentMessage) message).getXMLContent();
            return xml != null ? xml.getBytes(StandardCharsets.UTF_8) : new byte[0];
        } else if (message instanceof BytesMessage) {
            byte[] data = ((BytesMessage) message).getData();
            return data != null ? data : new byte[0];
        }
        byte[] data = message.getBytes();
        return data != null ? data : new byte[0];
    }

    /**
     * Infers an HTTP content type for a JCSMP message that carries no explicit HTTP content
     * type: XML for XMLContent messages, JSON/XML/plain by sniffing Text payloads, and
     * octet-stream otherwise.
     */
    public static String inferContentType(BytesXMLMessage message) {
        if (message instanceof XMLContentMessage) {
            return CONTENT_TYPE_XML;
        }
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            if (text != null) {
                String trimmed = text.trim();
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    return CONTENT_TYPE_JSON;
                }
                if (trimmed.startsWith("<")) {
                    return CONTENT_TYPE_XML;
                }
            }
            return CONTENT_TYPE_TEXT;
        }
        return "application/octet-stream";
    }

    /**
     * Returns true if the string looks like a JSON object or array, used to decide whether a
     * payload can be embedded as-is or must be quoted as a JSON string literal.
     */
    public static boolean looksLikeJson(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        String t = s.trim();
        return t.startsWith("{") || t.startsWith("[");
    }

    /**
     * Extracts the outgoing payload as a String and detects its content type in one pass.
     * Priority:
     *   1. SOAP body has a real XML first child (not Synapse's JSON-wrapper element)
     *      → serialize as XML. A {@code <text:message>} wrapper is unwrapped as plain text.
     *   2. JSON stream on the axis2 context → application/json
     *   3. Otherwise empty payload, text/plain
     *
     * <p>We intentionally check the envelope body before {@link JsonUtil#hasAJsonPayload}:
     * Synapse's REST API path exposes XML bodies as JSON for path-expression evaluation, which
     * flips {@code hasAJsonPayload} to {@code true} and would otherwise misclassify XML as JSON.
     *
     * @return a two-element array: [0]=payload string, [1]=detected content type
     */
    public static String[] extractPayloadAndContentType(MessageContext messageContext) {
        org.apache.axis2.context.MessageContext axis2Mc =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        OMElement first = messageContext.getEnvelope().getBody().getFirstElement();
        boolean hasJson = JsonUtil.hasAJsonPayload(axis2Mc);

        if (log.isDebugEnabled()) {
            log.debug("extractPayloadAndContentType: firstElement="
                    + (first == null ? "null" : first.getLocalName())
                    + ", hasAJsonPayload=" + hasJson);
        }

        // 1. Real XML / text-wrapper element wins (but skip JSON wrapper synthesized by Synapse)
        if (first != null && !isJsonWrapperElement(first)) {
            if ("message".equals(first.getLocalName())
                    && first.getNamespace() != null
                    && "http://ws.apache.org/commons/ns/payload".equals(
                            first.getNamespace().getNamespaceURI())) {
                String text = first.getText();
                return new String[] { text != null ? text : "", CONTENT_TYPE_TEXT };
            }
            return new String[] { first.toString(), CONTENT_TYPE_XML };
        }

        // 2. JSON stream
        if (hasJson) {
            String json = JsonUtil.jsonPayloadToString(axis2Mc);
            if (log.isDebugEnabled()) {
                log.debug("jsonPayloadToString returned: "
                        + (json == null ? "null" : "len=" + json.length() + " value=" + json));
            }
            if (json != null && !json.isEmpty()) {
                return new String[] { json, CONTENT_TYPE_JSON };
            }
            // Fallback: serialize the JSON OMElement wrapper back to JSON via formatter.
            if (first != null && isJsonWrapperElement(first)) {
                String fallback = jsonElementToString(first);
                if (log.isDebugEnabled()) {
                    log.debug("jsonElementToString fallback returned: "
                            + (fallback == null ? "null" : "len=" + fallback.length()
                                    + " value=" + fallback));
                }
                if (fallback != null && !fallback.isEmpty()) {
                    return new String[] { fallback, CONTENT_TYPE_JSON };
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("extractPayloadAndContentType: returning EMPTY payload as text/plain");
        }
        return new String[] { "", CONTENT_TYPE_TEXT };
    }

    /**
     * Serializes a Synapse JSON-wrapper OMElement ({@code <jsonObject>}/{@code <jsonArray>})
     * back to its JSON string form. Used as a fallback when the axis2 JSON input stream
     * has been consumed and {@link JsonUtil#jsonPayloadToString} returns empty.
     */
    private static String jsonElementToString(OMElement jsonElement) {
        try {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            JsonUtil.writeAsJson(jsonElement, out);
            return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to serialize JSON-wrapper element", e);
            }
            return null;
        }
    }

    private static boolean isJsonWrapperElement(OMElement element) {
        String local = element.getLocalName();
        return "jsonObject".equals(local) || "jsonArray".equals(local);
    }

    /**
     * Builds a {@link SolaceMessageProperties} from optional template parameters in the message context.
     * Returns {@code null} if no message properties were supplied — allowing callers to skip
     * applying properties entirely.
     *
     * @param messageContext the Synapse message context
     * @return populated SolaceMessageProperties, or {@code null} if none of the fields are set
     */
    public static SolaceMessageProperties buildMessageProperties(MessageContext messageContext) {
        SolaceMessageProperties properties = new SolaceMessageProperties();
        boolean hasProperties = false;

        String correlationId = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CORRELATION_ID);
        if (StringUtils.isNotEmpty(correlationId)) {
            properties.setCorrelationId(correlationId);
            hasProperties = true;
        }

        String priority = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.PRIORITY);
        Integer priorityValue = tryParseInt(priority, SolaceConstants.PRIORITY);
        if (priorityValue != null) {
            properties.setPriority(priorityValue);
            hasProperties = true;
        }

        String timeToLive = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.TIME_TO_LIVE);
        Long timeToLiveValue = tryParseLong(timeToLive, SolaceConstants.TIME_TO_LIVE);
        if (timeToLiveValue != null) {
            properties.setTimeToLive(timeToLiveValue);
            hasProperties = true;
        }

        String applicationMessageId = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.APPLICATION_MESSAGE_ID);
        if (StringUtils.isNotEmpty(applicationMessageId)) {
            properties.setApplicationMessageId(applicationMessageId);
            hasProperties = true;
        }

        String applicationMessageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.APPLICATION_MESSAGE_TYPE);
        if (StringUtils.isNotEmpty(applicationMessageType)) {
            properties.setApplicationMessageType(applicationMessageType);
            hasProperties = true;
        }

        String senderId = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SENDER_ID);
        if (StringUtils.isNotEmpty(senderId)) {
            properties.setSenderId(senderId);
            hasProperties = true;
        }

        String replyToDestinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.REPLY_TO_DESTINATION_TYPE);
        String replyToDestinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.REPLY_TO_DESTINATION_NAME);
        if (StringUtils.isNotEmpty(replyToDestinationType) && StringUtils.isNotEmpty(replyToDestinationName)) {
            properties.setReplyToDestinationType(replyToDestinationType);
            properties.setReplyToDestinationName(replyToDestinationName);
            hasProperties = true;
        }

        String userPropertiesJson = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.USER_PROPERTIES);
        if (StringUtils.isNotEmpty(userPropertiesJson)) {
            try {
            org.json.JSONArray jsonArray = new org.json.JSONArray(userPropertiesJson);
            Map<String, String> userPropsMap = new HashMap<>();
            for (int i = 0; i < jsonArray.length(); i++) {
                org.json.JSONObject obj = jsonArray.getJSONObject(i);
                String key = obj.keys().next();
                String value = obj.getString(key);
                userPropsMap.put(key, value);
            }
            properties.setUserProperties(userPropsMap);
            hasProperties = true;
            } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to parse user properties JSON array: " + userPropertiesJson, e);
            }
            }
        }

        String dmqEligible = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DMQ_ELIGIBLE);
        if (StringUtils.isNotEmpty(dmqEligible)) {
            properties.setDmqEligible(Boolean.parseBoolean(dmqEligible));
            hasProperties = true;
        }

        String elidingEligible = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.ELIDING_ELIGIBLE);
        if (StringUtils.isNotEmpty(elidingEligible)) {
            properties.setElidingEligible(Boolean.parseBoolean(elidingEligible));
            hasProperties = true;
        }

        String expiration = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.EXPIRATION);
        Long expirationValue = tryParseLong(expiration, SolaceConstants.EXPIRATION);
        if (expirationValue != null) {
            properties.setExpiration(expirationValue);
            hasProperties = true;
        }

        return hasProperties ? properties : null;
    }

    /**
     * Populates {@code sink} with the standard SMF headers from a received message
     * (poll/browse/sendRequest reply/inbound). Null/zero/default fields are skipped so
     * the response variable stays uncluttered. User properties (the SDTMap set by the
     * publisher via {@code <userProperties>}) are placed under the {@code userProperties}
     * key as a nested map. Pre-existing keys in {@code sink} (e.g. a destination name
     * the caller already filled in) are not overwritten.
     */
    public static void populateMessageMetadata(BytesXMLMessage message, Map<String, Object> sink) {
        if (message == null || sink == null) {
            return;
        }
        putIfAbsentNotNull(sink, "messageId", message.getMessageId());
        putIfAbsentNotNull(sink, "correlationId", message.getCorrelationId());
        putIfAbsentNotNull(sink, "applicationMessageId", message.getApplicationMessageId());
        putIfAbsentNotNull(sink, "applicationMessageType", message.getApplicationMessageType());
        putIfAbsentNotNull(sink, "senderId", message.getSenderId());
        if (message.getDeliveryMode() != null) {
            sink.putIfAbsent("deliveryMode", message.getDeliveryMode().name());
        }
        sink.putIfAbsent("priority", message.getPriority());
        sink.putIfAbsent("redelivered", message.getRedelivered());
        sink.putIfAbsent("dmqEligible", message.isDMQEligible());
        sink.putIfAbsent("elidingEligible", message.isElidingEligible());

        long expiration = message.getExpiration();
        if (expiration != 0L) {
            sink.putIfAbsent("expiration", expiration);
        }
        long timeToLive = message.getTimeToLive();
        if (timeToLive != 0L) {
            sink.putIfAbsent("timeToLive", timeToLive);
        }
        Long sendTs = message.getSenderTimestamp();
        if (sendTs != null) {
            sink.putIfAbsent("senderTimestamp", sendTs);
        }
        long receiveTs = message.getReceiveTimestamp();
        if (receiveTs != 0L) {
            sink.putIfAbsent("receiveTimestamp", receiveTs);
        }
        Long seqNum = message.getSequenceNumber();
        if (seqNum != null) {
            sink.putIfAbsent("sequenceNumber", seqNum);
        }

        Destination replyTo = message.getReplyTo();
        if (replyTo != null && !sink.containsKey("replyTo")) {
            Map<String, Object> replyToMap = new HashMap<>();
            replyToMap.put("name", replyTo.getName());
            replyToMap.put("type", replyTo instanceof Queue
                    ? SolaceConstants.DESTINATION_TYPE_QUEUE
                    : SolaceConstants.DESTINATION_TYPE_TOPIC);
            sink.put("replyTo", replyToMap);
        }

        Map<String, Object> userProps = extractUserProperties(message);
        if (userProps != null && !userProps.isEmpty() && !sink.containsKey("userProperties")) {
            sink.put("userProperties", userProps);
        }
    }

    /**
     * Reads the SDTMap of user properties (publisher-supplied {@code <userProperties>})
     * off a received message and converts it to a plain {@code Map<String, Object>} so
     * Synapse expressions can drill into it via {@code ${vars.X.attributes.userProperties.key}}.
     * Returns {@code null} if the message carries no user properties; primitive SDT values
     * (string/int/long/double/bool/byte[]) round-trip naturally; nested SDT containers fall
     * back to {@code toString()} so the field is at least readable.
     */
    public static Map<String, Object> extractUserProperties(BytesXMLMessage message) {
        if (message == null) {
            return null;
        }
        SDTMap props = message.getProperties();
        if (props == null || props.isEmpty()) {
            return null;
        }
        Map<String, Object> out = new HashMap<>();
        for (String key : props.keySet()) {
            try {
                Object value = props.get(key);
                if (value == null
                        || value instanceof String
                        || value instanceof Number
                        || value instanceof Boolean) {
                    out.put(key, value);
                } else if (value instanceof byte[]) {
                    out.put(key, java.util.Base64.getEncoder().encodeToString((byte[]) value));
                } else {
                    out.put(key, value.toString());
                }
            } catch (SDTException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to read user property '" + key + "'", e);
                }
            }
        }
        return out;
    }

    private static void putIfAbsentNotNull(Map<String, Object> sink, String key, Object value) {
        if (value != null) {
            sink.putIfAbsent(key, value);
        }
    }

    /**
     * Builds JCSMPProperties from the Synapse MessageContext parameters.
     *
     * @param messageContext the message context containing connection parameters
     * @return populated JCSMPProperties
     */
    public static JCSMPProperties buildJCSMPProperties(MessageContext messageContext) {

        JCSMPProperties properties = new JCSMPProperties();

        // Required properties
        String host = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.HOST);
        if (StringUtils.isNotEmpty(host)) {
            properties.setProperty(JCSMPProperties.HOST, host);
        }

        String vpnName = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.VPN_NAME);
        if (StringUtils.isNotEmpty(vpnName)) {
            properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);
        } else {
            properties.setProperty(JCSMPProperties.VPN_NAME, SolaceConstants.DEFAULT_VPN_NAME);
        }

        // Best practice: re-apply subscriptions after reconnect
        properties.setBooleanProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);

        String username = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.USERNAME);
        if (StringUtils.isNotEmpty(username)) {
            properties.setProperty(JCSMPProperties.USERNAME, username);
        }

        String password = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.PASSWORD);
        if (StringUtils.isNotEmpty(password)) {
            properties.setProperty(JCSMPProperties.PASSWORD, password);
        }

        // The client name is set per session by the logical connection (guide 3.3), never here.

        // Channel properties (timeouts, retries, compression)
        JCSMPChannelProperties channelProps = (JCSMPChannelProperties) properties.getProperty(
                JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);

        // Timeouts
        String connectionTimeout = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CONNECTION_TIMEOUT);
        Integer connectionTimeoutValue = tryParseInt(connectionTimeout, SolaceConstants.CONNECTION_TIMEOUT);
        if (connectionTimeoutValue != null) {
            channelProps.setConnectTimeoutInMillis(connectionTimeoutValue);
        }

        String readTimeout = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.READ_TIMEOUT);
        Integer readTimeoutValue = tryParseInt(readTimeout, SolaceConstants.READ_TIMEOUT);
        if (readTimeoutValue != null) {
            channelProps.setReadTimeoutInMillis(readTimeoutValue);
        }

        // Compression
        String compressionLevel = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.COMPRESSION_LEVEL);
        Integer compressionLevelValue = tryParseInt(compressionLevel, SolaceConstants.COMPRESSION_LEVEL);
        if (compressionLevelValue != null) {
            channelProps.setCompressionLevel(compressionLevelValue);
        }

        // Timestamps and sequence numbers
        String generateSendTimestamps = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.GENERATE_SEND_TIMESTAMPS);
        if (StringUtils.isNotEmpty(generateSendTimestamps)) {
            properties.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS,
                    Boolean.parseBoolean(generateSendTimestamps));
        }

        String generateReceiveTimestamps = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.GENERATE_RECEIVE_TIMESTAMPS);
        if (StringUtils.isNotEmpty(generateReceiveTimestamps)) {
            properties.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS,
                    Boolean.parseBoolean(generateReceiveTimestamps));
        }

        String generateSequenceNumbers = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.GENERATE_SEQUENCE_NUMBERS);
        if (StringUtils.isNotEmpty(generateSequenceNumbers)) {
            properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS,
                    Boolean.parseBoolean(generateSequenceNumbers));
        }

        String calculateMessageExpiration = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CALCULATE_MESSAGE_EXPIRATION);
        if (StringUtils.isNotEmpty(calculateMessageExpiration)) {
            properties.setBooleanProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION,
                    Boolean.parseBoolean(calculateMessageExpiration));
        }

        // Connection retry settings
        String connectRetries = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CONNECT_RETRIES);
        Integer connectRetriesValue = tryParseInt(connectRetries, SolaceConstants.CONNECT_RETRIES);
        if (connectRetriesValue != null) {
            channelProps.setConnectRetries(connectRetriesValue);
        }

        String connectRetriesPerHost = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CONNECT_RETRIES_PER_HOST);
        Integer connectRetriesPerHostValue = tryParseInt(connectRetriesPerHost,
                SolaceConstants.CONNECT_RETRIES_PER_HOST);
        if (connectRetriesPerHostValue != null) {
            channelProps.setConnectRetriesPerHost(connectRetriesPerHostValue);
        }

        String reconnectRetries = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.RECONNECT_RETRIES);
        Integer reconnectRetriesValue = tryParseInt(reconnectRetries, SolaceConstants.RECONNECT_RETRIES);
        if (reconnectRetriesValue != null) {
            channelProps.setReconnectRetries(reconnectRetriesValue);
        }

        String reconnectRetryWait = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.RECONNECT_RETRY_WAIT);
        Integer reconnectRetryWaitValue = tryParseInt(reconnectRetryWait, SolaceConstants.RECONNECT_RETRY_WAIT);
        if (reconnectRetryWaitValue != null) {
            channelProps.setReconnectRetryWaitInMillis(reconnectRetryWaitValue);
        }

        // SSL/TLS properties
        String sslTrustStorePath = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_TRUST_STORE_PATH);
        if (StringUtils.isNotEmpty(sslTrustStorePath)) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, sslTrustStorePath);
        }

        String sslTrustStorePassword = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_TRUST_STORE_PASSWORD);
        if (StringUtils.isNotEmpty(sslTrustStorePassword)) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, sslTrustStorePassword);
        }

        String sslTrustStoreFormat = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_TRUST_STORE_FORMAT);
        if (StringUtils.isNotEmpty(sslTrustStoreFormat)) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT, sslTrustStoreFormat);
        }

        String sslKeyStorePath = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_KEY_STORE_PATH);
        if (StringUtils.isNotEmpty(sslKeyStorePath)) {
            properties.setProperty(JCSMPProperties.SSL_KEY_STORE, sslKeyStorePath);
        }

        String sslKeyStorePassword = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_KEY_STORE_PASSWORD);
        if (StringUtils.isNotEmpty(sslKeyStorePassword)) {
            properties.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD, sslKeyStorePassword);
        }

        String sslKeyStoreFormat = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_KEY_STORE_FORMAT);
        if (StringUtils.isNotEmpty(sslKeyStoreFormat)) {
            properties.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT, sslKeyStoreFormat);
        }

        String sslKeyPassword = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_KEY_PASSWORD);
        if (StringUtils.isNotEmpty(sslKeyPassword)) {
            properties.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD, sslKeyPassword);
        }

        // Authentication scheme — defaults to BASIC if unset
        String authScheme = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.AUTHENTICATION_SCHEME);
        if (StringUtils.isNotEmpty(authScheme)) {
            switch (authScheme.toUpperCase()) {
                case SolaceConstants.AUTH_SCHEME_CLIENT_CERTIFICATE:
                    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                            JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE);
                    // JCSMP requires a non-null USERNAME to build the SMF login frame, even when authentication is by client certificate. 
                    String apiProvidedUsername = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                            SolaceConstants.API_PROVIDED_USERNAME);
                    if (StringUtils.isNotEmpty(apiProvidedUsername)) {     
                        properties.setProperty(JCSMPProperties.USERNAME, apiProvidedUsername);
                    } else {
                        // JCSMP NPEs on null USERNAME during SMF login frame build; broker ignores this
                        // value when username-source=common-name (default) and derives the identity from the TLS-presented cert.
                        properties.setProperty(JCSMPProperties.USERNAME, "<client-cert-cn>");
                    }
                    break;
                case SolaceConstants.AUTH_SCHEME_OAUTH2:
                    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                            JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
                    // JCSMP NPEs on null USERNAME during SMF login frame build; broker ignores this.
                    // A dummy value to avoid the NPE; the real OAuth2 token is sent in the OAUTH2_ACCESS_TOKEN property and the broker authenticates based on that.
                    properties.setProperty(JCSMPProperties.USERNAME, "<oauth2-token>");        
                    break;
                case SolaceConstants.AUTH_SCHEME_BASIC:
                default:
                    // JCSMP default — don't override; lets username/password drive auth
                    break;
            }
        }

        // OAuth2 / OIDC credentials — only meaningful under AUTHENTICATION_SCHEME == OAUTH2
        String oauth2AccessToken = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.OAUTH2_ACCESS_TOKEN);
        if (StringUtils.isNotEmpty(oauth2AccessToken)) {
            properties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oauth2AccessToken);
        }

        String oauth2IssuerIdentifier = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.OAUTH2_ISSUER_IDENTIFIER);
        if (StringUtils.isNotEmpty(oauth2IssuerIdentifier)) {
            properties.setProperty(JCSMPProperties.OAUTH2_ISSUER_IDENTIFIER, oauth2IssuerIdentifier);
        }

        String oidcIdToken = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.OIDC_ID_TOKEN);
        if (StringUtils.isNotEmpty(oidcIdToken)) {
            properties.setProperty(JCSMPProperties.OIDC_ID_TOKEN, oidcIdToken);
        }

        String sslValidateCertificate = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_VALIDATE_CERTIFICATE);
        if (StringUtils.isNotEmpty(sslValidateCertificate)) {
            boolean validateCertificate = Boolean.parseBoolean(sslValidateCertificate);
            if (!validateCertificate) {
                log.warn("Solace connection: server certificate validation is DISABLED "
                        + "(sslValidateCertificate=false). The broker's TLS certificate will not be "
                        + "verified, exposing the connection to man-in-the-middle attacks. Do not use "
                        + "this setting in production.");
            }
            properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, validateCertificate);
        }

        String sslValidateCertificateDate = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_VALIDATE_CERTIFICATE_DATE);
        if (StringUtils.isNotEmpty(sslValidateCertificateDate)) {
            properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE,
                    Boolean.parseBoolean(sslValidateCertificateDate));
        }

        String sslValidateHostname = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.SSL_VALIDATE_HOSTNAME);
        if (StringUtils.isNotEmpty(sslValidateHostname)) {
            properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_HOST,
                    Boolean.parseBoolean(sslValidateHostname));
        }

        // Publisher acknowledgement window size for guaranteed messaging throughput
        String pubAckWindowSize = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.PUB_ACK_WINDOW_SIZE);
        Integer pubAckWindowSizeValue = tryParseInt(pubAckWindowSize, SolaceConstants.PUB_ACK_WINDOW_SIZE);
        if (pubAckWindowSizeValue != null) {
            properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, pubAckWindowSizeValue);
        }

        return properties;
    }
}
