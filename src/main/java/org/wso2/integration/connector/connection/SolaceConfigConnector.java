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

import static org.wso2.integration.connector.constants.SolaceConstants.DEFAULT_POOL_MAX_WAIT_MS;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;
import static org.wso2.integration.connector.core.util.ConnectorUtils.getPoolConfiguration;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.pool.Configuration;
import org.wso2.integration.connector.core.util.ConnectorUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Initializes and manages the Solace connection.
 * This is the "init" operation of the connector that creates a session to the Solace broker.
 */
public class SolaceConfigConnector extends AbstractConnector implements ManagedLifecycle {

    private static final Log log = LogFactory.getLog(SolaceConfigConnector.class);
    // The lifecycle destroy and an orphaned logical connection's retire share one body, run once.
    private static final AtomicBoolean destroyedOnce = new AtomicBoolean();

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {

        String connectionName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
            SolaceConstants.NAME);
        if (StringUtils.isEmpty(connectionName)) {
            connectionName = SolaceConstants.CONNECTOR_NAME;
        }
        // The one context property the operations read. It is a name, never a credential.
        messageContext.setProperty(SolaceConstants.NAME, connectionName);

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        // One creation per name at a time. No broker work inside: the Context is local and the sessions are lazy.
        synchronized (LogicalConnections.lockFor(connectionName)) {
            // Check if connection already exists
            if (!handler.checkIfConnectionExists(SolaceConstants.CONNECTOR_NAME, connectionName)) {
                // A malformed connection never initialises: the template is validated before any logical
                // connection or pool exists.
                ConnectionSettings settings = ConnectionSettings.parse(messageContext);
                if (!settings.errors.isEmpty()) {
                    String text = FAULT_INVALID_CONFIG + ": " + String.join("; ", settings.errors);
                    ConnectorUtils.setErrorPropertiesToMessage(messageContext, String.valueOf(ERROR_CODE_INVALID_CONFIG),
                            text);
                    throw new ConnectException(text);
                }
                // Closes a previous logical connection of that name and creates the new one from this template.
                SolaceLogicalConnection lc = LogicalConnections.create(connectionName, messageContext);
                Configuration cfg = getPoolConfiguration(messageContext);
                // commons-pool 1.5.6: only a value greater than zero selects the timed wait.
                if (cfg.getMaxWaitTime() == null || cfg.getMaxWaitTime() <= 0) {
                    if (cfg.getMaxWaitTime() != null) {
                        log.warn("SolaceConnector [" + connectionName + "] maxWaitTime " + cfg.getMaxWaitTime()
                                + " is unlimited in commons-pool (zero or negative waits forever); using "
                                + DEFAULT_POOL_MAX_WAIT_MS + " ms");
                    }
                    cfg.setMaxWaitTime(Long.valueOf(DEFAULT_POOL_MAX_WAIT_MS));
                }
                if (cfg.getExhaustedAction() == null) {
                    cfg.setExhaustedAction("WHEN_EXHAUSTED_BLOCK");
                }
                // The context aware overload: connector core registers the local entry mapping and its undeploy
                // observer, so a local entry undeploy removes this pool.
                handler.createConnection(SolaceConstants.CONNECTOR_NAME, connectionName,
                        new SolaceConnectionFactory(lc), cfg, messageContext);
                // After the pool exists. A check between create() and createConnection would close the new
                // logical connection.
                lc.armExistenceCheck();
                log.info("SolaceConnector [" + connectionName + "] initialised (pool maxActive "
                        + (cfg.getMaxActiveConnections() == null ? "8 (default)" : cfg.getMaxActiveConnections())
                        + ", maxWait " + cfg.getMaxWaitTime() + " ms, "
                        + (settings.pooledSessionLimit == Integer.MAX_VALUE
                                ? "no limit on the pooled physical sessions because the pool can grow"
                                : "at most " + settings.pooledSessionLimit + " pooled physical sessions")
                        + ")");
                if (settings.clientNameStrict) {
                    // Strict mode gives pooled session 1 the name verbatim, and a duplicate name loops on
                    // evictions.
                    log.warn("SolaceConnector [" + connectionName + "] clientNameStrict=true: pooled session 1 uses "
                            + "the client name '" + settings.clientName + "' as is; a second session with this name "
                            + "on the broker evicts it and the two evict each other in turn; keep it unique per node");
                }
            }
        }

        if (log.isDebugEnabled()) {
            String host = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.HOST);
            String vpnName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.VPN_NAME);
            log.debug("Solace connection initialized - Host: " + host + ", VPN: " + vpnName
                + ", Name: " + connectionName);
        }
    }

    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        // Nothing to initialize
    }

    @Override
    public void destroy() {
        destroyOnce();
    }

    /** Once per connector library. SolaceLogicalConnection.retireOrphan() runs it for an orphan. */
    static void destroyOnce() {
        if (!destroyedOnce.compareAndSet(false, true)) {
            return;
        }
        // Stop the transaction watchdog first, so a timeout-triggered auto-rollback can't race
        // with the pool shutdown below — and so its daemon thread doesn't outlive the connector
        // (which would leak the thread and pin the classloader on redeploy).
        try {
            SolaceTransactionRegistry.shutdown();
        } catch (Exception e) {
            log.error("Error shutting down Solace transaction watchdog", e);
        }
        // Close all connections on destroy
        try {
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            handler.shutdownConnections(SolaceConstants.CONNECTOR_NAME);
            log.info("Solace connector connections shut down.");
        } catch (Exception e) {
            log.error("Error shutting down Solace connections", e);
        }
        // Each close stops admission and lets its closer run until the last owner detaches.
        try {
            LogicalConnections.closeAll();
        } catch (Exception e) {
            log.error("Error closing Solace logical connections", e);
        }
    }
}
