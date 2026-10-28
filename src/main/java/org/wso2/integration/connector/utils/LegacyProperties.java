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

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.synapse.MessageContext;

import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_CORRELATION_ID;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_DELIVERY_MODE;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_DESTINATION;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_MESSAGE_ID;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_REDELIVERED;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_REPLY_RECEIVE_TIMESTAMP;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_SENDER_ID;

/**
 * The legacy solace.* message context properties that poll and sendRequest write. We write them as one set on every
 * result: a field the new message lacks is removed from the context, so a value of an earlier message never survives
 * a later result, and a timed out poll removes the whole set. The settlement properties, the inbound's stashed
 * message and handle, the poll stash, the inbound transaction executor and the transaction id, are not part of the
 * set and are never touched here.
 */
public final class LegacyProperties {

    private static final String[] SET = {
            SOLACE_MESSAGE_ID, SOLACE_CORRELATION_ID, SOLACE_APPLICATION_MESSAGE_ID, SOLACE_APPLICATION_MESSAGE_TYPE,
            SOLACE_SENDER_ID, SOLACE_DELIVERY_MODE, SOLACE_DESTINATION, SOLACE_REDELIVERED,
            SOLACE_REPLY_RECEIVE_TIMESTAMP
    };

    private LegacyProperties() {
    }

    /** A poll result. A null message is a timed out poll and removes the set. */
    public static void writePolled(MessageContext ctx, BytesXMLMessage message, String queueName) {
        if (message == null) {
            clear(ctx);
            return;
        }
        String deliveryMode = message.getDeliveryMode() != null ? message.getDeliveryMode().name() : null;
        write(ctx, message, queueName, deliveryMode, null);
    }

    /** A sendRequest result: the destination and delivery mode of the request, the identity of the reply. */
    public static void writeReply(MessageContext ctx, BytesXMLMessage reply, String destinationName,
                                  String deliveryMode) {
        Long receiveTimestamp = reply.getReceiveTimestamp() > 0 ? reply.getReceiveTimestamp() : null;
        write(ctx, reply, destinationName, deliveryMode, receiveTimestamp);
    }

    /** Removes the whole set. */
    public static void clear(MessageContext ctx) {
        for (String name : SET) {
            ctx.getPropertyKeySet().remove(name);
        }
    }

    private static void write(MessageContext ctx, BytesXMLMessage message, String destination, String deliveryMode,
                              Long replyReceiveTimestamp) {
        put(ctx, SOLACE_MESSAGE_ID, message.getMessageId());
        put(ctx, SOLACE_CORRELATION_ID, message.getCorrelationId());
        put(ctx, SOLACE_APPLICATION_MESSAGE_ID, message.getApplicationMessageId());
        put(ctx, SOLACE_APPLICATION_MESSAGE_TYPE, message.getApplicationMessageType());
        put(ctx, SOLACE_SENDER_ID, message.getSenderId());
        put(ctx, SOLACE_DELIVERY_MODE, deliveryMode);
        put(ctx, SOLACE_DESTINATION, destination);
        put(ctx, SOLACE_REDELIVERED, message.getRedelivered());
        put(ctx, SOLACE_REPLY_RECEIVE_TIMESTAMP, replyReceiveTimestamp);
    }

    private static void put(MessageContext ctx, String name, Object value) {
        if (value == null) {
            ctx.getPropertyKeySet().remove(name);
        } else {
            ctx.setProperty(name, value);
        }
    }
}
