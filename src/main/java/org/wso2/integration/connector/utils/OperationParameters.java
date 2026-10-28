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

import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.util.ConnectorUtils;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.Locale;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;

/**
 * The strict parse of the operation parameters. An absent or blank value keeps the operation's default. An explicit
 * value is trimmed and must be one of the accepted words, true or false, or a whole number inside the range. Any
 * other value faults INVALID_CONFIG naming the parameter and the value. Every operation parses its parameters first,
 * before it reads a handle, settles a lease or calls the broker, so a bad value changes nothing.
 */
public final class OperationParameters {

    private OperationParameters() {
    }

    /** The settlement target of acknowledgeMessage, nackMessage and sendReply. Absent means INBOUND. */
    public static String target(MessageContext ctx) throws SolaceFault {
        return choice(ctx, SolaceConstants.TARGET, SolaceConstants.TARGET_INBOUND,
                SolaceConstants.TARGET_INBOUND, SolaceConstants.TARGET_POLL);
    }

    /** The delivery mode of a publish, a request or a reply. Absent means DIRECT. */
    public static String deliveryMode(MessageContext ctx) throws SolaceFault {
        return choice(ctx, SolaceConstants.DELIVERY_MODE, SolaceConstants.DELIVERY_MODE_DIRECT,
                SolaceConstants.DELIVERY_MODE_PERSISTENT, SolaceConstants.DELIVERY_MODE_NON_PERSISTENT,
                SolaceConstants.DELIVERY_MODE_DIRECT);
    }

    /** The destination type of a publish or a request. Absent means TOPIC. */
    public static String destinationType(MessageContext ctx) throws SolaceFault {
        return choice(ctx, SolaceConstants.DESTINATION_TYPE, SolaceConstants.DESTINATION_TYPE_TOPIC,
                SolaceConstants.DESTINATION_TYPE_TOPIC, SolaceConstants.DESTINATION_TYPE_QUEUE);
    }

    /** The message type of a publish, a request or a reply. Absent means TEXT. */
    public static String messageType(MessageContext ctx) throws SolaceFault {
        return choice(ctx, SolaceConstants.MESSAGE_TYPE, SolaceConstants.MESSAGE_TYPE_TEXT,
                SolaceConstants.MESSAGE_TYPE_TEXT, SolaceConstants.MESSAGE_TYPE_BYTES,
                SolaceConstants.MESSAGE_TYPE_XML);
    }

    /** The publish acknowledgement wait in milliseconds, 1 to 2147483647. */
    public static long ackTimeout(MessageContext ctx) throws SolaceFault {
        return longValue(ctx, SolaceConstants.ACK_TIMEOUT, SolaceConstants.DEFAULT_ACK_TIMEOUT_MS, 1L,
                Integer.MAX_VALUE);
    }

    /**
     * The message properties that buildMessageProperties reads. We check them here first, so the publish operations
     * fault on a bad value before any message is built. The read in buildMessageProperties then only sees values
     * this check accepted.
     */
    public static void checkMessageProperties(MessageContext ctx) throws SolaceFault {
        intValue(ctx, SolaceConstants.PRIORITY, 0, 0, 255);
        longValue(ctx, SolaceConstants.TIME_TO_LIVE, 0L, 0L, Long.MAX_VALUE);
        longValue(ctx, SolaceConstants.EXPIRATION, 0L, 0L, Long.MAX_VALUE);
        flag(ctx, SolaceConstants.DMQ_ELIGIBLE, false);
        flag(ctx, SolaceConstants.ELIDING_ELIGIBLE, false);
        choice(ctx, SolaceConstants.REPLY_TO_DESTINATION_TYPE, SolaceConstants.DESTINATION_TYPE_TOPIC,
                SolaceConstants.DESTINATION_TYPE_TOPIC, SolaceConstants.DESTINATION_TYPE_QUEUE);
    }

    /**
     * The contentType parameter of poll, sendRequest and browse, the reader override. Absent or blank means no
     * override and returns null.
     */
    public static String contentType(MessageContext ctx) throws SolaceFault {
        return parseContentType(SolaceConstants.CONTENT_TYPE, lookup(ctx, SolaceConstants.CONTENT_TYPE));
    }

    /**
     * A media type after trim: a type and a subtype with optional parameters, as HTTP defines it. A charset it
     * names must be one Java knows, and an empty charset names none. The result is the trimmed value as written,
     * null for a blank value.
     */
    public static String parseContentType(String name, String raw) throws SolaceFault {
        if (isBlank(raw)) {
            return null;
        }
        String value = raw.trim();
        if (!isMediaType(value)) {
            throw invalid(name + " must be a media type such as application/json, got '" + raw + "'");
        }
        // A charset parameter that is present with an empty value names no charset Java knows.
        String charset = MediaType.parse(value).charsetParameter();
        if (charset != null) {
            boolean known;
            try {
                known = Charset.isSupported(charset);
            } catch (IllegalCharsetNameException e) {
                known = false;
            }
            if (!known) {
                throw invalid(name + " names the unknown charset '" + charset + "', got '" + raw + "'");
            }
        }
        return value;
    }

    /** One of the accepted words after trim, case insensitive. The result is the accepted word as declared. */
    public static String choice(MessageContext ctx, String name, String defaultValue, String... accepted)
            throws SolaceFault {
        return parseChoice(name, lookup(ctx, name), defaultValue, accepted);
    }

    /** true or false after trim, case insensitive. */
    public static boolean flag(MessageContext ctx, String name, boolean defaultValue) throws SolaceFault {
        return parseFlag(name, lookup(ctx, name), defaultValue);
    }

    /** A whole number from min to max. The value is parsed as an int, never narrowed from a long. */
    public static int intValue(MessageContext ctx, String name, int defaultValue, int min, int max)
            throws SolaceFault {
        return parseInt(name, lookup(ctx, name), defaultValue, min, max);
    }

    /** A whole number from min to max. */
    public static long longValue(MessageContext ctx, String name, long defaultValue, long min, long max)
            throws SolaceFault {
        return parseLong(name, lookup(ctx, name), defaultValue, min, max);
    }

    public static String parseChoice(String name, String raw, String defaultValue, String... accepted)
            throws SolaceFault {
        if (isBlank(raw)) {
            return defaultValue;
        }
        String value = raw.trim().toUpperCase(Locale.ROOT);
        for (String candidate : accepted) {
            if (candidate.equals(value)) {
                return candidate;
            }
        }
        throw invalid(name + " must be " + words(accepted) + ", got '" + raw + "'");
    }

    public static boolean parseFlag(String name, String raw, boolean defaultValue) throws SolaceFault {
        if (isBlank(raw)) {
            return defaultValue;
        }
        String value = raw.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw invalid(name + " must be true or false, got '" + raw + "'");
    }

    public static int parseInt(String name, String raw, int defaultValue, int min, int max) throws SolaceFault {
        if (isBlank(raw)) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw invalid(name + " must be a whole number " + range(min, max) + ", got '" + raw + "'");
        }
        if (value < min || value > max) {
            throw invalid(name + " must be a whole number " + range(min, max) + ", got '" + raw + "'");
        }
        return value;
    }

    public static long parseLong(String name, String raw, long defaultValue, long min, long max) throws SolaceFault {
        if (isBlank(raw)) {
            return defaultValue;
        }
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw invalid(name + " must be a whole number " + range(min, max) + ", got '" + raw + "'");
        }
        if (value < min || value > max) {
            throw invalid(name + " must be a whole number " + range(min, max) + ", got '" + raw + "'");
        }
        return value;
    }

    private static String lookup(MessageContext ctx, String name) {
        Object value = ConnectorUtils.lookupTemplateParamater(ctx, name);
        return value == null ? null : value.toString();
    }

    private static boolean isBlank(String raw) {
        return raw == null || raw.trim().isEmpty();
    }

    // The media type grammar of HTTP: type "/" subtype, then any number of OWS ";" OWS with an optional
    // parameter, a parameter being a token "=" and a token or a quoted string.
    private static boolean isMediaType(String value) {
        int i = token(value, 0);
        if (i == 0 || i == value.length() || value.charAt(i) != '/') {
            return false;
        }
        int end = token(value, i + 1);
        if (end == i + 1) {
            return false;
        }
        i = end;
        while (true) {
            i = whitespace(value, i);
            if (i == value.length()) {
                return true;
            }
            if (value.charAt(i) != ';') {
                return false;
            }
            i = whitespace(value, i + 1);
            if (i == value.length() || value.charAt(i) == ';') {
                continue;
            }
            end = token(value, i);
            if (end == i || end == value.length() || value.charAt(end) != '=') {
                return false;
            }
            i = end + 1;
            end = i < value.length() && value.charAt(i) == '"' ? quotedString(value, i) : token(value, i);
            if (end <= i) {
                return false;
            }
            i = end;
        }
    }

    /** The index after the token that starts at from, from itself when no token starts there. */
    private static int token(String value, int from) {
        int i = from;
        while (i < value.length() && isTokenChar(value.charAt(i))) {
            i++;
        }
        return i;
    }

    private static boolean isTokenChar(char c) {
        return c > 0x20 && c < 0x7F && "()<>@,;:\\\"/[]?={}".indexOf(c) < 0;
    }

    /** The index after the quoted string that starts at from, minus one when it is not closed or not valid. */
    private static int quotedString(String value, int from) {
        int i = from + 1;
        while (i < value.length()) {
            char c = value.charAt(i);
            if (c == '"') {
                return i + 1;
            }
            if (c == '\\') {
                i++;
                if (i == value.length() || !isQuotedChar(value.charAt(i))) {
                    return -1;
                }
            } else if (!isQuotedChar(c)) {
                return -1;
            }
            i++;
        }
        return -1;
    }

    // A tab, a space, a visible ASCII character or obs text.
    private static boolean isQuotedChar(char c) {
        return c == '\t' || (c >= 0x20 && c != 0x7F && c <= 0xFF);
    }

    private static int whitespace(String value, int from) {
        int i = from;
        while (i < value.length() && (value.charAt(i) == ' ' || value.charAt(i) == '\t')) {
            i++;
        }
        return i;
    }

    private static String words(String... accepted) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < accepted.length; i++) {
            if (i > 0) {
                text.append(i == accepted.length - 1 ? " or " : ", ");
            }
            text.append(accepted[i]);
        }
        return text.toString();
    }

    private static String range(long min, long max) {
        if (max == Long.MAX_VALUE) {
            return "of " + min + " or more";
        }
        return "from " + min + " to " + max;
    }

    private static SolaceFault invalid(String message) {
        return new SolaceFault(FAULT_INVALID_CONFIG, ERROR_CODE_INVALID_CONFIG, message);
    }
}
