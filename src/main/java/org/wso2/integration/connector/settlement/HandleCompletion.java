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
package org.wso2.integration.connector.settlement;

import org.apache.commons.logging.Log;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.hooks.Hooks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.wso2.integration.connector.constants.SolaceConstants.REASON_ALREADY_DECIDED;
import static org.wso2.integration.connector.constants.SolaceConstants.REASON_LATE;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_INBOUND_SETTLEMENT;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_INBOUND_TX;
import static org.wso2.integration.connector.constants.SolaceConstants.SOLACE_MESSAGE_ID;

/**
 * Completion of the inbound endpoint's settlement handle and the shared result reporting of the settlement
 * operations (guide 3.2, design 7.4). The value functions are the connector's copy of the inbound's
 * SettlementValues. The string format of the handle value is the contract between the two bundles: an operation
 * writes ACK, FAILED or REJECTED, the inbound writes its cause followed by a colon and the decision time in
 * milliseconds.
 */
public final class HandleCompletion {

    public static final String VALUE_ACK = "ACK";
    public static final String VALUE_FAILED = "FAILED";
    public static final String VALUE_REJECTED = "REJECTED";
    public static final String CAUSE_TIMEOUT = "TIMEOUT";
    public static final String CAUSE_DRAIN = "DRAIN";
    public static final String CAUSE_INJECTION_REJECTED = "INJECTION_REJECTED";
    public static final String CAUSE_AUTOACK = "AUTOACK";
    public static final String CAUSE_OTHER = "OTHER";

    private HandleCompletion() {
    }

    /** Any mediation thread. Returns null when the context carries no handle (inbound 1.x). */
    public static SettlementResult completeInbound(MessageContext ctx, String outcome, String opName, Log log) {
        Object stashed = ctx.getProperty(SOLACE_INBOUND_SETTLEMENT);
        if (!(stashed instanceof CompletableFuture)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        CompletableFuture<String> handle = (CompletableFuture<String>) stashed;
        // Transacted deliveries only: the only handles with a measurement record (C.5).
        boolean transacted = ctx.getProperty(SOLACE_INBOUND_TX) instanceof Executor;
        // Test build: this attempt's instant, taken immediately before its complete() call. Shipped: null, no
        // clock read.
        Object attemptInstant = transacted ? Hooks.get().point("metrics.beforeComplete", handle) : null;
        // First writer wins. Only the winner publishes its instant, after it has won. A losing attempt never
        // touches the record.
        if (handle.complete(outcome)) {
            if (attemptInstant != null) {
                Hooks.get().point("metrics.handleCompleted", handle, attemptInstant);
            }
            return SettlementResult.deferred(outcome);
        }
        String stored = handle.getNow(null);
        // The worker's own record: TIMEOUT, DRAIN, INJECTION_REJECTED or AUTOACK.
        if (isInboundRecord(stored)) {
            long age = ageMillis(stored);
            log.warn("solace." + opName + ": late " + outcome + " for message " + messageId(ctx) + ": the inbound decided "
                    + cause(stored) + " " + age + " ms ago; nothing reaches the broker");
            return SettlementResult.notSettled(outcome, REASON_LATE, age);
        }
        // An earlier connector completion: a clone branch, or a second ack in a fault sequence.
        return SettlementResult.notSettled(outcome, REASON_ALREADY_DECIDED, -1);
    }

    private static Object messageId(MessageContext ctx) {
        return ctx.getProperty(SOLACE_MESSAGE_ID);
    }

    public static String cause(String value) {
        if (value == null) {
            return CAUSE_OTHER;
        }
        if (value.equals(VALUE_ACK) || value.equals(VALUE_FAILED) || value.equals(VALUE_REJECTED)) {
            return value;
        }
        int separator = value.indexOf(':');
        String prefix = separator < 0 ? value : value.substring(0, separator);
        return (prefix.equals(CAUSE_TIMEOUT) || prefix.equals(CAUSE_DRAIN) || prefix.equals(CAUSE_INJECTION_REJECTED)
                || prefix.equals(CAUSE_AUTOACK)) ? prefix : CAUSE_OTHER;
    }

    /** Minus one for an operation written value, which carries no timestamp. */
    public static long ageMillis(String value) {
        int separator = value.indexOf(':');
        return separator < 0 ? -1 : System.currentTimeMillis() - Long.parseLong(value.substring(separator + 1));
    }

    public static boolean isInboundRecord(String value) {
        return value != null && value.indexOf(':') > 0;
    }

    /**
     * The response payload of acknowledgeMessage and nackMessage: settled, outcome, mode, reason when not settled,
     * ageMillis when LATE, ackMessageId when DIRECT and not zero, settledAt.
     */
    public static JSONObject payload(SettlementResult result) {
        JSONObject payload = new JSONObject();
        payload.put("settled", result.settled);
        payload.put("outcome", result.outcome);
        payload.put("mode", result.mode);
        if (result.reason != null) {
            payload.put("reason", result.reason);
        }
        if (REASON_LATE.equals(result.reason)) {
            payload.put("ageMillis", result.ageMillis);
        }
        if (result.ackMessageId != 0) {
            payload.put("ackMessageId", result.ackMessageId);
        }
        payload.put("settledAt", System.currentTimeMillis());
        return payload;
    }

    /** The response attributes of acknowledgeMessage and nackMessage: settled, outcome, mode, reason. */
    public static Map<String, Object> attributes(SettlementResult result) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("settled", result.settled);
        attributes.put("outcome", result.outcome);
        attributes.put("mode", result.mode);
        if (result.reason != null) {
            attributes.put("reason", result.reason);
        }
        if (REASON_LATE.equals(result.reason)) {
            attributes.put("ageMillis", result.ageMillis);
        }
        return attributes;
    }
}
