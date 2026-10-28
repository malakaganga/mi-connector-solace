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

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.SolaceMessageProperties;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PAYLOAD_ERROR;

/**
 * Utility class for building JCSMP properties from the Synapse MessageContext.
 */
public final class SolaceUtils {

    private static final Log log = LogFactory.getLog(SolaceUtils.class);

    private SolaceUtils() {
        // Prevent instantiation
    }

    // ---- Safe numeric parsing -----------------------------------------------------------
    // User-supplied parameters arrive as strings. A malformed value must not crash the whole
    // operation with an uncaught NumberFormatException — instead we log a WARN and fall back
    // to a sensible value (a caller-provided default, or skipping an optional setting).

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

    /**
     * Builds a {@link SolaceMessageProperties} from optional template parameters in the message context.
     * Returns {@code null} if no message properties were supplied — allowing callers to skip
     * applying properties entirely.
     *
     * @param messageContext the Synapse message context
     * @param operation      the operation name, for the fault message
     * @return populated SolaceMessageProperties, or {@code null} if none of the fields are set
     * @throws SolaceFault PAYLOAD_ERROR when the userProperties input is not the documented JSON array, and
     *                     INVALID_CONFIG for a bad number, boolean or reply to type
     */
    public static SolaceMessageProperties buildMessageProperties(MessageContext messageContext, String operation)
            throws SolaceFault {
        SolaceMessageProperties properties = new SolaceMessageProperties();
        boolean hasProperties = false;

        String correlationId = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.CORRELATION_ID);
        if (StringUtils.isNotEmpty(correlationId)) {
            properties.setCorrelationId(correlationId);
            hasProperties = true;
        }

        // The numbers and the booleans use the strict parse of the operation parameters. An absent value sets
        // nothing, a bad value faults INVALID_CONFIG before any message is built.
        String priority = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.PRIORITY);
        if (StringUtils.isNotBlank(priority)) {
            properties.setPriority(OperationParameters.parseInt(SolaceConstants.PRIORITY, priority, 0, 0, 255));
            hasProperties = true;
        }

        String timeToLive = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.TIME_TO_LIVE);
        if (StringUtils.isNotBlank(timeToLive)) {
            properties.setTimeToLive(OperationParameters.parseLong(SolaceConstants.TIME_TO_LIVE, timeToLive, 0L, 0L,
                    Long.MAX_VALUE));
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
        if (StringUtils.isNotEmpty(replyToDestinationName)) {
            // An absent reply to type means TOPIC, as for the destination type.
            properties.setReplyToDestinationType(OperationParameters.parseChoice(
                    SolaceConstants.REPLY_TO_DESTINATION_TYPE, replyToDestinationType,
                    SolaceConstants.DESTINATION_TYPE_TOPIC, SolaceConstants.DESTINATION_TYPE_TOPIC,
                    SolaceConstants.DESTINATION_TYPE_QUEUE));
            properties.setReplyToDestinationName(replyToDestinationName);
            hasProperties = true;
        }

        String userPropertiesJson = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.USER_PROPERTIES);
        if (StringUtils.isNotEmpty(userPropertiesJson)) {
            properties.setUserProperties(parseUserProperties(userPropertiesJson, operation));
            hasProperties = true;
        }

        String dmqEligible = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.DMQ_ELIGIBLE);
        if (StringUtils.isNotBlank(dmqEligible)) {
            properties.setDmqEligible(OperationParameters.parseFlag(SolaceConstants.DMQ_ELIGIBLE, dmqEligible, false));
            hasProperties = true;
        }

        String elidingEligible = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.ELIDING_ELIGIBLE);
        if (StringUtils.isNotBlank(elidingEligible)) {
            properties.setElidingEligible(OperationParameters.parseFlag(SolaceConstants.ELIDING_ELIGIBLE,
                    elidingEligible, false));
            hasProperties = true;
        }

        String expiration = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.EXPIRATION);
        if (StringUtils.isNotBlank(expiration)) {
            properties.setExpiration(OperationParameters.parseLong(SolaceConstants.EXPIRATION, expiration, 0L, 0L,
                    Long.MAX_VALUE));
            hasProperties = true;
        }

        return hasProperties ? properties : null;
    }

    /**
     * Parses the userProperties input: a JSON array of objects, every key of every object is a property, so the
     * one key per object form keeps working. A string is a string property, an integer a long, a decimal a double
     * and a boolean a boolean. A null, a nested object or array, invalid JSON or an input that is not an array
     * faults PAYLOAD_ERROR, before any broker call. The keys keep their order.
     */
    public static Map<String, Object> parseUserProperties(String json, String operation) throws SolaceFault {
        JsonElement parsed = PayloadCodec.parseJson(json);
        if (parsed == null) {
            throw payloadFault(operation, "userProperties is not valid JSON: " + json);
        }
        if (!parsed.isJsonArray()) {
            throw payloadFault(operation, "userProperties must be a JSON array of objects, got " + json);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (JsonElement item : parsed.getAsJsonArray()) {
            if (!item.isJsonObject()) {
                throw payloadFault(operation, "every element of userProperties must be a JSON object, got " + item);
            }
            for (Map.Entry<String, JsonElement> entry : item.getAsJsonObject().entrySet()) {
                out.put(entry.getKey(), userPropertyValue(entry.getKey(), entry.getValue(), operation));
            }
        }
        return out;
    }

    private static Object userPropertyValue(String key, JsonElement value, String operation) throws SolaceFault {
        if (!value.isJsonPrimitive()) {
            throw payloadFault(operation, "user property '" + key + "' must be a string, a number or a boolean, got "
                    + value);
        }
        JsonPrimitive primitive = value.getAsJsonPrimitive();
        if (primitive.isBoolean()) {
            return primitive.getAsBoolean();
        }
        if (primitive.isNumber()) {
            String literal = primitive.getAsString();
            if (literal.matches("-?\\d+")) {
                try {
                    return Long.parseLong(literal);
                } catch (NumberFormatException e) {
                    throw payloadFault(operation, "user property '" + key + "' is outside the long range: " + literal);
                }
            }
            double decimal = primitive.getAsDouble();
            if (Double.isInfinite(decimal) || Double.isNaN(decimal)) {
                throw payloadFault(operation, "user property '" + key + "' is outside the double range: " + literal);
            }
            return decimal;
        }
        return primitive.getAsString();
    }

    private static SolaceFault payloadFault(String operation, String reason) {
        return new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR, operation + ": " + reason);
    }

    /**
     * Populates {@code sink} with the standard SMF headers of a received message, for poll, sendRequest and the
     * browse items. Every value is a string, a number or a boolean. Null, zero and default fields are skipped so
     * the response variable stays uncluttered. The reply to destination becomes {@code replyToName} and
     * {@code replyToType}. Every user property becomes its own attribute named {@code userProp_} followed by
     * the key, the key unchanged, so {@code ${vars.X.attributes.userProp_key}} reads it. A key in {@code sink}
     * that the caller already filled in, for example the destination, is not overwritten.
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
        if (replyTo != null) {
            putIfAbsentNotNull(sink, "replyToName", replyTo.getName());
            sink.putIfAbsent("replyToType", replyTo instanceof Queue
                    ? SolaceConstants.DESTINATION_TYPE_QUEUE
                    : SolaceConstants.DESTINATION_TYPE_TOPIC);
        }

        Map<String, Object> userProps = extractUserProperties(message);
        if (userProps != null) {
            for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                sink.put(SolaceConstants.USER_PROPERTY_PREFIX + entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Reads the SDTMap of user properties, the ones a publisher supplied through {@code <userProperties>}, off a
     * received message and converts it to a plain {@code Map<String, Object>} keyed by the property name. The
     * metadata helper puts every entry on the attributes as {@code userProp_} followed by the key, so a Synapse
     * expression reads it as {@code ${vars.X.attributes.userProp_key}}, or with the bracket form
     * {@code ${vars.X.attributes['userProp_a.b']}} for a key with punctuation. A string, a number and a boolean
     * stay as they are. A bytes value, the JCSMP ByteArray that the client returns or a byte array, becomes base64
     * text. A nested SDTMap becomes JSON object text and a nested SDTStream JSON array text. A null is skipped.
     * Returns {@code null} if the message carries no user properties.
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
                if (value == null) {
                    continue;
                }
                if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                    out.put(key, value);
                } else if (value instanceof SDTMap) {
                    out.put(key, PayloadCodec.sdtMapToJson((SDTMap) value).toString());
                } else if (value instanceof SDTStream) {
                    out.put(key, PayloadCodec.sdtStreamToJson((SDTStream) value).toString());
                } else {
                    out.put(key, PayloadCodec.sdtScalarToText(value));
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

        // The client name is set per session by the logical connection, never here.

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
