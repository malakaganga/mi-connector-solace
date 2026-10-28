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
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.hooks.Hooks;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.PayloadCodec;

import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PUBLISH_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PUBLISH_ERROR;

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
     * Resolves a delivery mode string into the JCSMP enum. Absent means DIRECT. The operations validate the value
     * before they build a message, so an unknown value here is a programming error and not a fallback.
     */
    public static DeliveryMode resolveDeliveryMode(String deliveryMode) {
        if (deliveryMode == null || deliveryMode.trim().isEmpty()) {
            return DeliveryMode.DIRECT;
        }
        switch (deliveryMode.trim().toUpperCase(Locale.ROOT)) {
            case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                return DeliveryMode.PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                return DeliveryMode.NON_PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_DIRECT:
                return DeliveryMode.DIRECT;
            default:
                throw new IllegalArgumentException("unknown deliveryMode '" + deliveryMode + "'");
        }
    }

    /**
     * Creates a message of the requested type from the outgoing body the platform formatter wrote. BYTES puts
     * the bytes into the BytesMessage as they are. TEXT and XML decode the bytes with the charset they were
     * written in. A binary body with TEXT or XML faults PAYLOAD_ERROR, before any broker call. JSON is carried
     * as TEXT on the wire, Solace has no JSON message type.
     *
     * @param messageType message type: TEXT, BYTES, or XML (default: TEXT)
     * @param payload     the outgoing body
     * @param synCtx      mediation context of the caller, used to propagate the trace context
     * @param operation   the operation name, for the fault message
     * @return the created message, carrying the trace context when tracing is enabled
     */
    public static BytesXMLMessage createMessage(String messageType, PayloadCodec.Outgoing payload,
                                                MessageContext synCtx, String operation) throws SolaceFault {
        if (messageType == null) {
            messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
        }
        BytesXMLMessage message;
        switch (messageType.toUpperCase()) {
            case SolaceConstants.MESSAGE_TYPE_BYTES:
                BytesMessage bytesMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                bytesMsg.setData(payload.bytes);
                message = bytesMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_XML:
                XMLContentMessage xmlMsg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
                xmlMsg.setXMLContent(textOf(payload, messageType, operation));
                message = xmlMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_TEXT:
            default:
                TextMessage textMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMsg.setText(textOf(payload, messageType, operation));
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

    private static String textOf(PayloadCodec.Outgoing payload, String messageType, String operation)
            throws SolaceFault {
        if (payload.binary) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR, operation
                    + ": the message body is binary (" + payload.mediaType + ") and messageType " + messageType
                    + " carries text, use messageType BYTES");
        }
        return PayloadCodec.decode(payload.bytes, payload.encoding, operation);
    }

    /**
     * Applies optional SolaceMessageProperties to a message. A user property is written with its type: a string,
     * a long, a double or a boolean. A failure to apply them faults PUBLISH_ERROR and nothing is sent.
     */
    public static void applyMessageProperties(BytesXMLMessage message, SolaceMessageProperties properties)
            throws SolaceFault {
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
        if (properties.getUserProperties() != null && !properties.getUserProperties().isEmpty()) {
            try {
                // A test build can inject an application failure here.
                Hooks.get().point("message.beforeSdtApply", message);
                SDTMap userPropsMap = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, Object> entry : properties.getUserProperties().entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof Boolean) {
                        userPropsMap.putBoolean(entry.getKey(), (Boolean) value);
                    } else if (value instanceof Long) {
                        userPropsMap.putLong(entry.getKey(), (Long) value);
                    } else if (value instanceof Double) {
                        userPropsMap.putDouble(entry.getKey(), (Double) value);
                    } else {
                        userPropsMap.putString(entry.getKey(), String.valueOf(value));
                    }
                }
                message.setProperties(userPropsMap);
            } catch (SDTException | RuntimeException e) {
                throw new SolaceFault(FAULT_PUBLISH_ERROR, ERROR_CODE_PUBLISH_ERROR,
                        "cannot apply the user properties to the message: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Resolves a destination (topic or queue) from type and name. Absent means TOPIC. The operations validate the
     * type before they build a message, so an unknown type here is a programming error and not a fallback.
     */
    public static Destination resolveDestination(String type, String name) {
        String resolved = type == null || type.trim().isEmpty()
                ? SolaceConstants.DESTINATION_TYPE_TOPIC : type.trim().toUpperCase(Locale.ROOT);
        switch (resolved) {
            case SolaceConstants.DESTINATION_TYPE_QUEUE:
                return JCSMPFactory.onlyInstance().createQueue(name);
            case SolaceConstants.DESTINATION_TYPE_TOPIC:
                return JCSMPFactory.onlyInstance().createTopic(name);
            default:
                throw new IllegalArgumentException("unknown destination type '" + type + "'");
        }
    }

    /** The one delivery mode switch, through resolveDeliveryMode. Absent means DIRECT. */
    public static void applyDeliveryMode(BytesXMLMessage message, String deliveryMode) {
        message.setDeliveryMode(resolveDeliveryMode(deliveryMode));
    }

    public static String newCorrelationKey(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
