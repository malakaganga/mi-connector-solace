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

import org.apache.commons.lang.StringUtils;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.utils.ClientNames;

import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.wso2.integration.connector.constants.SolaceConstants.AUTHENTICATION_SCHEME;
import static org.wso2.integration.connector.constants.SolaceConstants.AUTH_SCHEME_OAUTH2;
import static org.wso2.integration.connector.constants.SolaceConstants.CLIENT_NAME;
import static org.wso2.integration.connector.constants.SolaceConstants.CLIENT_NAME_STRICT;
import static org.wso2.integration.connector.constants.SolaceConstants.DEFAULT_MAX_CONCURRENT_REQUESTS;
import static org.wso2.integration.connector.constants.SolaceConstants.DEFAULT_MAX_POLL_FLOWS;
import static org.wso2.integration.connector.constants.SolaceConstants.DEFAULT_POLL_ACK_TIMEOUT_MS;
import static org.wso2.integration.connector.constants.SolaceConstants.DEFAULT_POLL_FLOW_IDLE_TIMEOUT_MS;
import static org.wso2.integration.connector.constants.SolaceConstants.KEEP_ALIVE_INTERVAL;
import static org.wso2.integration.connector.constants.SolaceConstants.KEEP_ALIVE_LIMIT;
import static org.wso2.integration.connector.constants.SolaceConstants.MAX_CONCURRENT_REQUESTS;
import static org.wso2.integration.connector.constants.SolaceConstants.MAX_POLL_FLOWS;
import static org.wso2.integration.connector.constants.SolaceConstants.OAUTH2_TOKEN_FILE;
import static org.wso2.integration.connector.constants.SolaceConstants.POLL_ACK_TIMEOUT;
import static org.wso2.integration.connector.constants.SolaceConstants.POLL_FLOW_IDLE_TIMEOUT;
import static org.wso2.integration.connector.constants.SolaceConstants.PUB_ACK_WINDOW_SIZE;
import static org.wso2.integration.connector.constants.SolaceConstants.RECOVERY_INITIAL_DELAY;
import static org.wso2.integration.connector.constants.SolaceConstants.RECOVERY_MAX_ATTEMPTS;
import static org.wso2.integration.connector.constants.SolaceConstants.RECOVERY_MAX_DELAY;
import static org.wso2.integration.connector.constants.SolaceConstants.RECOVERY_PROGRESSION;
import static org.wso2.integration.connector.constants.SolaceConstants.REQUIRE_OUTCOMES_AUTO;
import static org.wso2.integration.connector.constants.SolaceConstants.REQUIRE_OUTCOMES_TRUE;
import static org.wso2.integration.connector.constants.SolaceConstants.REQUIRE_SETTLEMENT_OUTCOMES;

/**
 * The 2.0.0 connection parameters of a logical connection, parsed once from the init template.
 * Parsing follows the inbound endpoint's rule: a default applies only to an absent or blank value, a malformed
 * present value is an error. parse() never throws, it collects every error for SolaceConfigConnector.connect().
 */
public final class ConnectionSettings {

    public final long pollAckTimeoutMillis;
    public final long pollFlowIdleTimeoutMillis;
    public final int maxPollFlows;
    public final String requireSettlementOutcomes;
    public final int maxConcurrentRequests;
    // The publisher window of every session, JCSMP's default of 1 unless the connection sets pubAckWindowSize.
    // At 1 the producer lock is held through the confirmation wait.
    public final int pubAckWindowSize;
    public final long recoveryInitialDelayMillis;
    public final double recoveryProgression;
    public final long recoveryMaxDelayMillis;
    public final int recoveryMaxAttempts;
    public final String clientName;
    public final boolean clientNameStrict;
    public final Path oauth2TokenFile;
    // Minus one leaves the JCSMP default.
    public final long keepAliveIntervalMillis;
    public final int keepAliveLimit;
    public final List<String> errors;

    private ConnectionSettings(MessageContext messageContext) {
        List<String> collected = new ArrayList<>();

        this.pollAckTimeoutMillis = parseLong(parameter(messageContext, POLL_ACK_TIMEOUT), POLL_ACK_TIMEOUT,
                DEFAULT_POLL_ACK_TIMEOUT_MS, collected);
        this.pollFlowIdleTimeoutMillis = parseLong(parameter(messageContext, POLL_FLOW_IDLE_TIMEOUT),
                POLL_FLOW_IDLE_TIMEOUT, DEFAULT_POLL_FLOW_IDLE_TIMEOUT_MS, collected);
        this.maxPollFlows = parseInt(parameter(messageContext, MAX_POLL_FLOWS), MAX_POLL_FLOWS,
                DEFAULT_MAX_POLL_FLOWS, collected);
        this.requireSettlementOutcomes = enumValue(parameter(messageContext, REQUIRE_SETTLEMENT_OUTCOMES),
                REQUIRE_SETTLEMENT_OUTCOMES, REQUIRE_OUTCOMES_AUTO, collected, REQUIRE_OUTCOMES_AUTO,
                REQUIRE_OUTCOMES_TRUE);
        this.maxConcurrentRequests = parseInt(parameter(messageContext, MAX_CONCURRENT_REQUESTS),
                MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS, collected);
        this.pubAckWindowSize = parseInt(parameter(messageContext, PUB_ACK_WINDOW_SIZE), PUB_ACK_WINDOW_SIZE, 1,
                collected);
        // The range checks after the parses. No timer of the connector reads 0 as disabled.
        if (pollAckTimeoutMillis < 1) {
            collected.add(POLL_ACK_TIMEOUT + " must be at least 1, got " + pollAckTimeoutMillis);
        }
        if (pollFlowIdleTimeoutMillis < 1) {
            collected.add(POLL_FLOW_IDLE_TIMEOUT + " must be at least 1, got " + pollFlowIdleTimeoutMillis);
        }
        if (maxPollFlows < 1) {
            collected.add(MAX_POLL_FLOWS + " must be at least 1, got " + maxPollFlows);
        }
        if (maxConcurrentRequests < 1) {
            collected.add(MAX_CONCURRENT_REQUESTS + " must be at least 1, got " + maxConcurrentRequests);
        }
        if (pubAckWindowSize < 1 || pubAckWindowSize > 255) {
            collected.add(PUB_ACK_WINDOW_SIZE + " must be between 1 and 255, got " + pubAckWindowSize);
        }

        this.recoveryInitialDelayMillis = parseLong(parameter(messageContext, RECOVERY_INITIAL_DELAY),
                RECOVERY_INITIAL_DELAY, 1000, collected);
        this.recoveryProgression = parseDouble(parameter(messageContext, RECOVERY_PROGRESSION),
                RECOVERY_PROGRESSION, 2.0, collected);
        this.recoveryMaxDelayMillis = parseLong(parameter(messageContext, RECOVERY_MAX_DELAY),
                RECOVERY_MAX_DELAY, 60000, collected);
        checkBackoff(RECOVERY_INITIAL_DELAY, recoveryInitialDelayMillis, RECOVERY_PROGRESSION,
                recoveryProgression, RECOVERY_MAX_DELAY, recoveryMaxDelayMillis, collected);
        this.recoveryMaxAttempts = parseInt(parameter(messageContext, RECOVERY_MAX_ATTEMPTS),
                RECOVERY_MAX_ATTEMPTS, -1, collected);
        if (recoveryMaxAttempts < -1) {
            collected.add(RECOVERY_MAX_ATTEMPTS + " must be at least -1, got " + recoveryMaxAttempts);
        }

        String rawClientName = parameter(messageContext, CLIENT_NAME);
        this.clientName = StringUtils.isBlank(rawClientName) ? null : rawClientName.trim();
        this.clientNameStrict = parseBoolean(parameter(messageContext, CLIENT_NAME_STRICT), CLIENT_NAME_STRICT,
                false, collected);
        if (clientNameStrict && clientName != null) {
            int bytes = clientName.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > ClientNames.MAX_CLIENT_NAME_BYTES) {
                collected.add(CLIENT_NAME + " must be at most " + ClientNames.MAX_CLIENT_NAME_BYTES + " bytes with "
                        + CLIENT_NAME_STRICT + "=true, got " + bytes + " bytes");
            }
        }

        this.keepAliveIntervalMillis = parseLong(parameter(messageContext, KEEP_ALIVE_INTERVAL), KEEP_ALIVE_INTERVAL,
                -1, collected);
        if (keepAliveIntervalMillis != -1
                && (keepAliveIntervalMillis < 50 || keepAliveIntervalMillis > Integer.MAX_VALUE)) {
            collected.add(KEEP_ALIVE_INTERVAL + " must be at least 50 ms and at most " + Integer.MAX_VALUE
                    + ", got " + keepAliveIntervalMillis);
        }
        this.keepAliveLimit = parseInt(parameter(messageContext, KEEP_ALIVE_LIMIT), KEEP_ALIVE_LIMIT, -1, collected);
        if (keepAliveLimit != -1 && keepAliveLimit < 1) {
            collected.add(KEEP_ALIVE_LIMIT + " must be at least 1, got " + keepAliveLimit);
        }

        String tokenFile = parameter(messageContext, OAUTH2_TOKEN_FILE);
        String authScheme = parameter(messageContext, AUTHENTICATION_SCHEME);
        Path tokenPath = null;
        if (StringUtils.isNotBlank(tokenFile)) {
            try {
                tokenPath = Paths.get(tokenFile.trim());
            } catch (InvalidPathException e) {
                collected.add("invalid value '" + tokenFile + "' for " + OAUTH2_TOKEN_FILE);
            }
            if (!AUTH_SCHEME_OAUTH2.equalsIgnoreCase(authScheme)) {
                collected.add(OAUTH2_TOKEN_FILE + " requires " + AUTHENTICATION_SCHEME + "=" + AUTH_SCHEME_OAUTH2);
            }
        }
        this.oauth2TokenFile = tokenPath;

        this.errors = Collections.unmodifiableList(collected);
    }

    public static ConnectionSettings parse(MessageContext messageContext) {
        return new ConnectionSettings(messageContext);
    }

    private static String parameter(MessageContext messageContext, String name) {
        Object value = ConnectorUtils.lookupTemplateParamater(messageContext, name);
        return value == null ? null : value.toString();
    }

    private static int parseInt(String value, String name, int defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    private static long parseLong(String value, String name, long defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    private static double parseDouble(String value, String name, double defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            double parsed = Double.parseDouble(value.trim());
            if (!Double.isFinite(parsed)) {
                throw new NumberFormatException(value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    private static boolean parseBoolean(String value, String name, boolean defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return true;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return false;
        }
        errors.add("invalid value '" + value + "' for " + name);
        return defaultValue;
    }

    private static String enumValue(String value, String name, String defaultValue, List<String> errors,
                                    String... allowed) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        String upper = value.trim().toUpperCase(Locale.ROOT);
        if (Arrays.asList(allowed).contains(upper)) {
            return upper;
        }
        errors.add(name + " must be one of " + String.join(", ", allowed) + ", got '" + value + "'");
        return defaultValue;
    }

    private static void checkBackoff(String initialName, long initial, String progressionName, double progression,
                                     String maxName, long max, List<String> errors) {
        if (initial < 0) {
            errors.add(initialName + " must be at least 0, got " + initial);
        }
        if (progression < 1.0) {
            errors.add(progressionName + " must be at least 1.0, got " + progression);
        }
        if (max < initial) {
            errors.add(maxName + " must be at least " + initialName + " (" + initial + "), got " + max);
        }
    }
}
