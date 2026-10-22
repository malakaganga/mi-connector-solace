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
package org.wso2.integration.connector.connection;

import com.solace.messaging.trace.propagation.SolaceJCSMPTextMapSetter;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.tracing.opentelemetry.management.handling.span.SpanHandler;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.models.SolaceMessageProperties;

import java.util.Map;
import java.util.UUID;

/**
 * Message construction shared by the pooled, request reply and transacted publish paths.
 * The four 1.0.1 methods moved here from SolaceConnection without change.
 */
public final class MessageBuilder {

    private static final Log log = LogFactory.getLog(MessageBuilder.class);

    /**
     * Writes W3C trace context into a JCSMP message.
     */
    private static final SolaceJCSMPTextMapSetter TRACE_CONTEXT_SETTER = new SolaceJCSMPTextMapSetter();

    private MessageBuilder() {
    }

    /**
     * Resolves a delivery mode string into the JCSMP enum. Null or unknown falls back to DIRECT.
     */
    public static DeliveryMode resolveDeliveryMode(String deliveryMode) {
        if (deliveryMode == null) {
            return DeliveryMode.DIRECT;
        }
        switch (deliveryMode.toUpperCase()) {
            case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                return DeliveryMode.PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                return DeliveryMode.NON_PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_DIRECT:
            default:
                return DeliveryMode.DIRECT;
        }
    }

    /**
     * Creates a message of the specified type with the given payload. Supports TEXT, BYTES, and XML.
     * JSON payloads are carried as TEXT on the wire (Solace has no JSON message type); callers are
     * expected to pass messageType=TEXT for JSON content.
     *
     * @param messageType message type: TEXT, BYTES, or XML (default: TEXT)
     * @param payload     the message payload
     * @param synCtx      mediation context of the caller, used to propagate the trace context
     * @return the created message, carrying the trace context when tracing is enabled
     */
    public static BytesXMLMessage createMessage(String messageType, String payload, MessageContext synCtx) {
        if (messageType == null) {
            messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
        }
        BytesXMLMessage message;
        switch (messageType.toUpperCase()) {
            case SolaceConstants.MESSAGE_TYPE_BYTES:
                BytesMessage bytesMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                bytesMsg.setData(payload != null ? payload.getBytes() : new byte[0]);
                message = bytesMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_XML:
                XMLContentMessage xmlMsg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
                if (payload != null) {
                    xmlMsg.setXMLContent(payload);
                }
                message = xmlMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_TEXT:
            default:
                TextMessage textMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMsg.setText(payload);
                message = textMsg;
        }
        if (RuntimeStatisticCollector.isOpenTelemetryEnabled()) {
            try {
                SpanHandler.injectTraceContext(synCtx, message, TRACE_CONTEXT_SETTER);
            } catch (NoSuchMethodError e) {
                log.warn("You are running solace connector on old wso2mi version. " +
                        "Distributed tracing will not working correctly in this version. " +
                        "Please upgrade the wso2mi version");
            }
        }
        return message;
    }

    /**
     * Applies optional SolaceMessageProperties to a message.
     */
    public static void applyMessageProperties(BytesXMLMessage message, SolaceMessageProperties properties) {
        if (properties == null) {
            return;
        }
        if (properties.getCorrelationId() != null) {
            message.setCorrelationId(properties.getCorrelationId());
        }
        if (properties.getPriority() != null) {
            message.setPriority(properties.getPriority());
        }
        if (properties.getTimeToLive() != null) {
            message.setTimeToLive(properties.getTimeToLive());
        }
        if (properties.getApplicationMessageId() != null) {
            message.setApplicationMessageId(properties.getApplicationMessageId());
        }
        if (properties.getApplicationMessageType() != null) {
            message.setApplicationMessageType(properties.getApplicationMessageType());
        }
        if (properties.getSenderId() != null) {
            message.setSenderId(properties.getSenderId());
        }
        if (properties.getDmqEligible() != null) {
            message.setDMQEligible(properties.getDmqEligible());
        }
        if (properties.getElidingEligible() != null) {
            message.setElidingEligible(properties.getElidingEligible());
        }
        if (properties.getExpiration() != null) {
            message.setExpiration(properties.getExpiration());
        }
        if (properties.getReplyToDestinationType() != null && properties.getReplyToDestinationName() != null) {
            Destination replyTo = resolveDestination(properties.getReplyToDestinationType(),
                    properties.getReplyToDestinationName());
            message.setReplyTo(replyTo);
        }
        // Set user properties (custom key-value pairs)
        if (properties.getUserProperties() != null && !properties.getUserProperties().isEmpty()) {
            try {
                SDTMap userPropsMap = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, String> entry : properties.getUserProperties().entrySet()) {
                    userPropsMap.putString(entry.getKey(), entry.getValue());
                }
                message.setProperties(userPropsMap);
            } catch (SDTException e) {
                log.error("Error setting user properties on message", e);
            }
        }
    }

    /**
     * Resolves a destination (topic or queue) from type and name.
     */
    public static Destination resolveDestination(String type, String name) {
        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(type)) {
            return JCSMPFactory.onlyInstance().createQueue(name);
        }
        return JCSMPFactory.onlyInstance().createTopic(name);
    }

    /**
     * The one copy of the 1.0.1 delivery mode switch. A null mode is DIRECT, as the 1.0.1 reply path read it.
     */
    public static void applyDeliveryMode(BytesXMLMessage message, String deliveryMode) {
        if (deliveryMode == null) {
            message.setDeliveryMode(DeliveryMode.DIRECT);
            return;
        }
        switch (deliveryMode.toUpperCase()) {
            case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                message.setDeliveryMode(DeliveryMode.PERSISTENT);
                break;
            case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                message.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                break;
            case SolaceConstants.DELIVERY_MODE_DIRECT:
            default:
                message.setDeliveryMode(DeliveryMode.DIRECT);
                break;
        }
    }

    public static String newCorrelationKey(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
