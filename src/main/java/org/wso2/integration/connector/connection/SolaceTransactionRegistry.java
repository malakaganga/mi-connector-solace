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

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;

/**
 * Tracks active transactional connections by transaction id, with an auto-rollback
 * watchdog so a forgotten commit/rollback doesn't leak the transacted session.
 */
public final class SolaceTransactionRegistry {

    private static final Log log = LogFactory.getLog(SolaceTransactionRegistry.class);

    private static final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private static ScheduledExecutorService watchdog = newWatchdog();

    private SolaceTransactionRegistry() {}

    private static ScheduledExecutorService newWatchdog() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "solace-tx-watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Returns the watchdog scheduler, recreating it if a prior {@link #shutdown()} terminated
     * it. Guards against scheduling on a dead executor (RejectedExecutionException) when the
     * connector is used again after a teardown — e.g. a same-classloader redeploy, or
     * {@code destroy()} firing while the connector is still in use elsewhere.
     */
    private static synchronized ScheduledExecutorService watchdog() {
        if (watchdog.isShutdown()) {
            watchdog = newWatchdog();
        }
        return watchdog;
    }

    /**
     * Creates the entry, its executor and its thread. The entry is attached to the logical connection.
     * One protocol under the registry's lock, which shutdown() takes too: the entry enters the map, the timeout is
     * scheduled, the owner starts, so a timeout that fires always finds its entry. A scheduling or start failure
     * removes the entry, releases its attachment, stops its executor and faults the begin with the cause.
     */
    public static synchronized Entry register(SolaceConnection connection, String connectionName, long timeoutMillis)
            throws SolaceFault {
        String txId = UUID.randomUUID().toString();
        Entry entry = new Entry(connection, connectionName, txId);
        connection.lc.attach(entry);
        entries.put(txId, entry);
        try {
            entry.timeoutFuture = watchdog().schedule(() -> autoRollback(txId),
                    timeoutMillis, TimeUnit.MILLISECONDS);
            entry.executor.start();
        } catch (RuntimeException e) {
            entries.remove(txId);
            if (entry.timeoutFuture != null) {
                entry.timeoutFuture.cancel(false);
            }
            entry.executor.closeAdmission();
            entry.executor.finished = true;
            connection.lc.detach(entry);
            throw new SolaceFault(FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR,
                    "transaction registration failed on connection " + connection.getConnectionId() + ": " + e, e);
        }
        log.info("TransactionRegistry: registered txId=" + txId + " (connectionName=" + connectionName
                + ", connectionId=" + connection.getConnectionId() + ", timeoutMillis=" + timeoutMillis
                + ", activeCount=" + entries.size() + ")");
        return entry;
    }

    public static Entry get(String txId) {
        Entry e = entries.get(txId);
        if (e == null) {
            log.debug("TransactionRegistry: lookup miss for txId=" + txId
                    + " (activeCount=" + entries.size() + ")");
            return null;
        }
        return e;
    }

    public static Entry unregister(String txId) {
        Entry e = entries.remove(txId);
        if (e != null && e.timeoutFuture != null) {
            e.timeoutFuture.cancel(false);
            log.debug("TransactionRegistry: unregistered txId=" + txId
                    + " (connectionName=" + e.connectionName + ", activeCount=" + entries.size() + ")");
            return e;
        }
        log.debug("TransactionRegistry: unregister miss for txId=" + txId
                + " (activeCount=" + entries.size() + ")");
        return null;
    }

    /**
     * Connector teardown (SolaceConfigConnector.destroy). Every open transaction is rolled back on its own
     * executor, which returns the holder and detaches the entry. Then the watchdog stops, so its daemon thread
     * does not outlive the connector's classloader.
     */
    public static synchronized void shutdown() {
        for (String txId : new ArrayList<>(entries.keySet())) {
            Entry entry = unregister(txId);
            if (entry == null) {
                continue;
            }
            entry.executor.execute(entry.executor.completionCommand(false));
            log.warn("solace transaction " + txId + " rolled back: connector undeployed");
        }
        watchdog.shutdownNow();
        entries.clear();
        log.info("TransactionRegistry: watchdog shut down.");
    }

    /** Watchdog thread. Nothing runs here but the submission. */
    private static void autoRollback(String txId) {
        Entry e = entries.remove(txId);
        if (e == null) return;
        log.warn("Transaction " + txId + " timed out — auto-rolling back (connectionName="
                + e.connectionName + ", connectionId=" + e.connection.getConnectionId() + ")");
        e.executor.execute(e.executor.completionCommand(false));
    }

    public static class Entry {
        public final SolaceConnection connection;
        public final String connectionName;
        public final String txId;
        public final TransactionExecutor executor;
        // Set by closeAdmission(): no command runs after it.
        volatile boolean terminal;
        // Set when a command was cut off while running: the completion rolls back even on commit.
        volatile boolean uncertain;
        ScheduledFuture<?> timeoutFuture;
        Entry(SolaceConnection c, String n, String txId) {
            this.connection = c;
            this.connectionName = n;
            this.txId = txId;
            this.executor = new TransactionExecutor(this, txId, c.lc);
        }
    }
}
