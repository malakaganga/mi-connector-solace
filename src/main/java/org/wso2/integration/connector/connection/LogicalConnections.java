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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

/**
 * The static registry of logical connections by connection name.
 */
public final class LogicalConnections {

    private static final Log log = LogFactory.getLog(LogicalConnections.class);

    private static final ConcurrentHashMap<String, SolaceLogicalConnection> byName = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    private LogicalConnections() {
    }

    /** The per name lock that create(), returnToPool() and SolaceConfigConnector.connect() share. */
    public static Object lockFor(String name) {
        return locks.computeIfAbsent(name, key -> new Object());
    }

    /**
     * Closes and removes an existing entry of that name, creates the new one and publishes it.
     * A Context and a watchdog are created, no broker work happens.
     */
    public static SolaceLogicalConnection create(String name, MessageContext messageContext) {
        synchronized (lockFor(name)) {
            SolaceLogicalConnection previous = byName.remove(name);
            if (previous != null) {
                previous.close();
            }
            SolaceLogicalConnection created = new SolaceLogicalConnection(name, messageContext);
            byName.put(name, created);
            return created;
        }
    }

    public static SolaceLogicalConnection get(String name) {
        return byName.get(name);
    }

    /**
     * Removes and closes only when the mapped instance is expected, so a stale check never removes a replacement.
     * The close is submitted to the closer, because the caller is the watchdog thread.
     */
    public static void remove(String name, SolaceLogicalConnection expected) {
        if (!byName.remove(name, expected)) {
            return;
        }
        try {
            expected.closer.execute(expected::close);
        } catch (RejectedExecutionException e) {
            log.debug("SolaceConnector [" + name + "] closer already shut down; the logical connection is closed", e);
        }
    }

    public static void closeAll() {
        List<Map.Entry<String, SolaceLogicalConnection>> entries = new ArrayList<>(byName.entrySet());
        for (Map.Entry<String, SolaceLogicalConnection> entry : entries) {
            if (byName.remove(entry.getKey(), entry.getValue())) {
                entry.getValue().close();
            }
        }
    }
}
