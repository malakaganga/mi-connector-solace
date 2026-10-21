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

package org.wso2.integration.connector.operation;

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.connection.TransactionBridge;
import org.wso2.integration.connector.connection.TransactionCommand;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.concurrent.Executor;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_CONNECTION_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.TX_COMPLETION_WAIT_MS;

public class SolaceBeginTransaction extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        // Nested begin would orphan the outer tx — its TX_CONNECTION_ID would be
        // overwritten and the outer pinned connection could only be reclaimed by the watchdog.
        log.debug("solace.beginTransaction: invoked");
        // A connector owned transaction cannot be nested inside an inbound owned one (G11).
        if (messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX) instanceof Executor) {
            fault(FAULT_INBOUND_TRANSACTION, ERROR_CODE_INBOUND_TRANSACTION,
                    "beginTransaction is not supported inside an inbound-owned transaction (G11)", null, messageContext);
            return;
        }
        String existingTxId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (existingTxId != null) {
            log.error("solace.beginTransaction: rejected — active transaction already in scope: " + existingTxId);
            throw new ConnectException(
                    "solace.beginTransaction called inside an active transaction scope: " + existingTxId);
        }
        String connectionName = null;
        SolaceConnection connection = null;
        boolean registered = false;
        try {
            connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
            if (connectionName == null) {
                handleException("Connection name is not set.", messageContext);
                return;
            }

            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, SolaceConstants.TX_TIMEOUT_MILLIS);
            long timeoutMillis = SolaceUtils.parseLongOrDefault(timeoutStr,
                    Long.parseLong(SolaceConstants.DEFAULT_TX_TIMEOUT_MILLIS),
                    SolaceConstants.TX_TIMEOUT_MILLIS);
            log.debug("solace.beginTransaction: connectionName=" + connectionName
                    + ", timeoutMillis=" + timeoutMillis);

            connection = (SolaceConnection) ConnectionHandler.getConnectionHandler()
                    .getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (connection == null) {
                log.error("solace.beginTransaction: connection '" + connectionName + "' not available from pool");
                throw new ConnectException("Solace connection '" + connectionName + "' is not available");
            }
            log.debug("solace.beginTransaction: pinned connection acquired (connectionId="
                    + connection.getConnectionId() + ")");

            // Connects the holder on first use under an attempt record of the transaction's own timeout (3.3). The
            // record is finished before the session is handed to the transaction.
            connection.ensureSession(System.currentTimeMillis() + timeoutMillis);

            SolaceTransactionRegistry.Entry entry = SolaceTransactionRegistry.register(connection, connectionName,
                    timeoutMillis);
            registered = true;
            String txId = entry.txId;
            log.debug("solace.beginTransaction: registered txId=" + txId + " in TransactionRegistry");
            try {
                // The transacted session is created on the transaction's own thread, as every later access to it.
                TransactionBridge.submitAndWait(entry.executor,
                        new TransactionCommand<>(connection::getOrCreateTransactedSession, TX_COMPLETION_WAIT_MS),
                        "beginTransaction", FAULT_CONNECTION_ERROR, ERROR_CODE_CONNECTION_ERROR, messageContext);
            } catch (SolaceFault sessionFault) {
                // Once the entry exists, the completion command is the one release path: it returns the holder and
                // detaches the entry. Whoever wins the unregister submits it.
                if (SolaceTransactionRegistry.unregister(txId) != null) {
                    entry.executor.execute(entry.executor.completionCommand(false));
                }
                throw sessionFault;
            }
            log.debug("solace.beginTransaction: transacted session ready on connectionId="
                    + connection.getConnectionId());

            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .setProperty(SolaceConstants.TX_CONNECTION_ID, txId);

            log.info("solace.beginTransaction: started txId=" + txId + " (timeout=" + timeoutMillis + "ms)");
        } catch (SolaceFault fault) {
            // Before the entry exists the holder is returned here, the baseline rule.
            if (connection != null && !registered) {
                connection.returnToPool();
            }
            fault(fault, messageContext);
        } catch (Exception e) {
            log.error("solace.beginTransaction: failed (connectionName=" + connectionName + "): " + e.getMessage(), e);
            // Registration didn't happen → ownership stays with us, return to pool so the
            // connection isn't lost. Once registered, the registry owns the lifecycle
            // (commit/rollback/watchdog will release it).
            if (connection != null && !registered) {
                try {
                    connection.returnToPool();
                    log.error("solace.beginTransaction: returned unregistered connection to pool after failure");
                } catch (Exception releaseEx) {
                    log.warn("Failed to return Solace connection to pool after failed beginTransaction",
                            releaseEx);
                }
            }
            throw new ConnectException(e, "Failed to begin Solace transaction");
        }
    }

    private void fault(SolaceFault fault, MessageContext messageContext) {
        fault(fault.name, fault.code, fault.getMessage(), fault.getCause(), messageContext);
    }

    private void fault(String name, int code, String message, Throwable cause, MessageContext messageContext) {
        String text = name + ": " + message;
        ConnectorUtils.setErrorPropertiesToMessage(messageContext, String.valueOf(code), text);
        handleException(text, cause instanceof Exception ? (Exception) cause : null, messageContext);
    }
}
