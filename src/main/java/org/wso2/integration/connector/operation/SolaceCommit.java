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
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.connection.TransactionExecutor;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.util.ConnectorUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INBOUND_TRANSACTION;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.TX_COMPLETION_WAIT_MS;

public class SolaceCommit extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        log.debug("solace.commit: invoked");
        if (messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_TX) instanceof Executor) {
            fault(FAULT_INBOUND_TRANSACTION, ERROR_CODE_INBOUND_TRANSACTION,
                    "commit/rollback are the inbound's in transacted mode: acknowledge or nack the message instead",
                    null, messageContext);
            return;
        }
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (txId == null) {
            log.error("solace.commit: rejected — no transaction in scope");
            throw new ConnectException("solace.commit called outside a transaction scope");
        }
        log.debug("solace.commit: txId=" + txId + " resolved from message context");
        SolaceTransactionRegistry.Entry entry = SolaceTransactionRegistry.unregister(txId);
        if (entry == null) {
            log.error("solace.commit: txId=" + txId + " not found in registry"
                    + " (already committed/rolled back/timed out)");
            throw new ConnectException("Transaction " + txId + " not found (already committed/rolled back/timed out?)");
        }
        log.debug("solace.commit: txId=" + txId + " unregistered; committing on connectionId="
                + entry.connection.getConnectionId() + " (connectionName=" + entry.connectionName + ")");
        try {
            // The completion runs on the transaction's own thread and is never cancelled from here. The wait is
            // bounded, the completion is not.
            TransactionExecutor.CompletionCommand completion = entry.executor.completionCommand(true);
            entry.executor.execute(completion);
            completion.get(TX_COMPLETION_WAIT_MS, TimeUnit.MILLISECONDS);
            log.info("solace.commit: txId=" + txId + " committed successfully");
        } catch (TimeoutException e) {
            fault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN, "completion of transaction " + txId + " did not finish within "
                    + TX_COMPLETION_WAIT_MS + " ms; it continues on its owner", null, messageContext);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SolaceFault) {
                fault((SolaceFault) cause, messageContext);
            } else {
                fault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                        "commit of transaction " + txId + " failed; its result is unknown", cause, messageContext);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN, "completion of transaction " + txId
                    + " was interrupted while this operation waited, it continues on its owner", e, messageContext);
        } finally {
            // Registry entry is already gone — strip the context marker and release the
            // pinned connection regardless of commit outcome so the flow doesn't carry a
            // dead txId into the next operation.
            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .removeProperty(SolaceConstants.TX_CONNECTION_ID);
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
