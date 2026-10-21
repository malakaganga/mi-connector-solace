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

import org.apache.commons.lang.StringUtils;
import org.wso2.config.mapper.ConfigParser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Deterministic client names for the connector's sessions. The connector's copy of the inbound endpoint's class.
 * A rebuilt session reuses its predecessor's name so the broker evicts the zombie (design 8.1).
 */
public final class ClientNames {

    // Solace limits the client name in bytes, not in characters (G3).
    public static final int MAX_CLIENT_NAME_BYTES = 160;

    private static final int HASH_HEX_LENGTH = 8;
    private static final int PREFIX_BYTE_LIMIT = MAX_CLIENT_NAME_BYTES - 1 - HASH_HEX_LENGTH;

    private ClientNames() {
    }

    public static final class NodeId {
        public final String id;
        public final String source;

        NodeId(String id, String source) {
            this.id = id;
            this.source = source;
        }
    }

    public static NodeId nodeId() {
        String value = System.getProperty("node.id");
        if (StringUtils.isNotBlank(value)) {
            return new NodeId(value.trim(), "node.id");
        }
        value = System.getProperty("nodeId");
        if (StringUtils.isNotBlank(value)) {
            return new NodeId(value.trim(), "nodeId property");
        }
        value = System.getenv("nodeId");
        if (StringUtils.isNotBlank(value)) {
            return new NodeId(value.trim(), "nodeId env");
        }
        Object toml = null;
        try {
            toml = ConfigParser.getParsedConfigs().get("cluster_config.node_id");
        } catch (Throwable t) {
            // ConfigParser is absent or not initialised, so the host name is tried next.
        }
        if (toml != null && StringUtils.isNotBlank(toml.toString())) {
            return new NodeId(toml.toString().trim(), "deployment.toml");
        }
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (StringUtils.isNotBlank(host)) {
                return new NodeId(host.trim(), "hostname");
            }
        } catch (UnknownHostException | SecurityException e) {
            // The host name does not resolve, so the fallback applies.
        }
        return new NodeId("localhost", "fallback");
    }

    /**
     * Strict mode gives pooled session 1 the prefix verbatim and every other role the prefix plus the role,
     * because a pool cannot share one verbatim name (G9). A blank strict prefix leaves the name to JCSMP.
     */
    public static String connector(String prefix, String nodeId, String connectionName, String role, boolean strict) {
        if (strict) {
            if (StringUtils.isBlank(prefix)) {
                return null;
            }
            return "1".equals(role) ? prefix : prefix + "-" + role;
        }
        String base = (StringUtils.isBlank(prefix) ? "" : prefix + "-") + nodeId + "-" + connectionName + "-" + role;
        if (base.getBytes(StandardCharsets.UTF_8).length <= MAX_CLIENT_NAME_BYTES) {
            return base;
        }
        int end = 0;
        int bytes = 0;
        int i = 0;
        while (i < base.length()) {
            int codePoint = base.codePointAt(i);
            int codePointBytes = codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
            if (bytes + codePointBytes > PREFIX_BYTE_LIMIT) {
                break;
            }
            bytes += codePointBytes;
            i += Character.charCount(codePoint);
            end = i;
        }
        return base.substring(0, end) + "-" + sha256Hex(base).substring(0, HASH_HEX_LENGTH);
    }

    private static String sha256Hex(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte digestByte : digest) {
                hex.append(Character.forDigit((digestByte >> 4) & 0xF, 16))
                        .append(Character.forDigit(digestByte & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
