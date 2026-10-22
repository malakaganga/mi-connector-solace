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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads the OAuth2 token from a file at every connect. The connector's copy of the inbound's class.
 */
public final class OAuthTokenSource {

    private static final Log log = LogFactory.getLog(OAuthTokenSource.class);

    private final Path file;
    private final String staticToken;
    // The connection name for the log prefix.
    private final String name;
    private volatile String last;
    // One WARN per failure streak.
    private final AtomicBoolean failing = new AtomicBoolean();

    public OAuthTokenSource(Path file, String staticToken, String name) {
        this.file = file;
        this.staticToken = staticToken;
        this.name = name;
    }

    // Called on an operation thread at connect and on the JCSMP notification executor on RECONNECTING.
    public String current() {
        if (file == null) {
            return staticToken;
        }
        try {
            // The whole file is read and then trimmed, so an atomic replace is always seen complete.
            String token = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();
            if (token.isEmpty()) {
                throw new IOException("empty token file");
            }
            last = token;
            if (failing.compareAndSet(true, false)) {
                log.info("SolaceConnector [" + name + "] OAuth2 token file readable again: " + file);
            }
            return token;
        } catch (IOException | SecurityException e) {
            if (failing.compareAndSet(false, true)) {
                log.warn("SolaceConnector [" + name + "] OAuth2 token file " + file + " could not be read ("
                        + e.getMessage() + "); keeping the last token");
            }
            return last != null ? last : staticToken;
        }
    }
}
