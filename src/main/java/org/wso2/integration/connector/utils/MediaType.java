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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * A content type split once into its base type and its parameters, with the quoted string rules of HTTP. Inside
 * quotes a backslash escapes the next character, an escaped quote does not end the value, and a semicolon is
 * not a separator. A parameter name compares case insensitively and a quoted value is unquoted. The one parser
 * of the connector for the charset inheritance, the declaration given to a builder and the override.
 */
public final class MediaType {

    public static final String CHARSET_PARAMETER = "charset";

    /** The base type, lower case and trimmed. */
    public final String baseType;
    private final List<Parameter> parameters;

    private static final class Parameter {

        /** The name, lower case. */
        final String name;
        /** The value, unquoted and unescaped. */
        final String value;
        /** The parameter as written, trimmed, so a rebuilt type keeps it exactly. */
        final String text;

        Parameter(String name, String value, String text) {
            this.name = name;
            this.value = value;
            this.text = text;
        }
    }

    private MediaType(String baseType, List<Parameter> parameters) {
        this.baseType = baseType;
        this.parameters = parameters;
    }

    /**
     * Splits a content type. The base type is the text before the first semicolon. A parameter without a name
     * or without an equals sign is ignored. Null for a blank content type.
     */
    public static MediaType parse(String contentType) {
        if (contentType == null) {
            return null;
        }
        int separator = contentType.indexOf(';');
        String base = (separator < 0 ? contentType : contentType.substring(0, separator)).trim()
                .toLowerCase(Locale.ROOT);
        if (base.isEmpty()) {
            return null;
        }
        List<Parameter> parameters = new ArrayList<>();
        int i = separator;
        while (i >= 0 && i < contentType.length()) {
            // i is at a semicolon. The parameter runs to the next semicolon outside quotes.
            int start = i + 1;
            int equals = -1;
            int end = start;
            boolean quoted = false;
            while (end < contentType.length()) {
                char c = contentType.charAt(end);
                if (quoted) {
                    if (c == '\\') {
                        end++;
                    } else if (c == '"') {
                        quoted = false;
                    }
                } else if (c == '"') {
                    quoted = true;
                } else if (c == ';') {
                    break;
                } else if (c == '=' && equals < 0) {
                    equals = end;
                }
                end++;
            }
            end = Math.min(end, contentType.length());
            if (equals > start) {
                String name = contentType.substring(start, equals).trim().toLowerCase(Locale.ROOT);
                if (!name.isEmpty()) {
                    parameters.add(new Parameter(name, value(contentType.substring(equals + 1, end).trim()),
                            contentType.substring(start, end).trim()));
                }
            }
            i = end < contentType.length() ? end : -1;
        }
        return new MediaType(base, Collections.unmodifiableList(parameters));
    }

    /** A value with its quotes removed and its escapes resolved. A token is returned as it is. */
    private static String value(String raw) {
        if (raw.isEmpty() || raw.charAt(0) != '"') {
            return raw;
        }
        StringBuilder value = new StringBuilder();
        for (int i = 1; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\' && i + 1 < raw.length()) {
                value.append(raw.charAt(++i));
            } else if (c == '"') {
                break;
            } else {
                value.append(c);
            }
        }
        return value.toString();
    }

    /** The base type of a content type, lower case. Null for a blank content type. */
    public static String baseTypeOf(String contentType) {
        MediaType type = parse(contentType);
        return type == null ? null : type.baseType;
    }

    /**
     * The value of the first charset parameter, trimmed, an empty text when the parameter is present with an
     * empty value, null when no charset parameter is present.
     */
    public String charsetParameter() {
        for (Parameter parameter : parameters) {
            if (CHARSET_PARAMETER.equals(parameter.name)) {
                return parameter.value.trim();
            }
        }
        return null;
    }

    /** The charset the type names, null when it names none or an empty one. */
    public String charset() {
        String charset = charsetParameter();
        return charset == null || charset.isEmpty() ? null : charset;
    }

    /** The base type with every parameter except charset, as written. */
    public String withoutCharset() {
        StringBuilder type = new StringBuilder(baseType);
        for (Parameter parameter : parameters) {
            if (!CHARSET_PARAMETER.equals(parameter.name)) {
                type.append("; ").append(parameter.text);
            }
        }
        return type.toString();
    }

    /** The base type with every parameter except charset, as written, and one charset parameter at the end. */
    public String withCharset(String charset) {
        return withoutCharset() + "; " + CHARSET_PARAMETER + "=" + charset;
    }
}
