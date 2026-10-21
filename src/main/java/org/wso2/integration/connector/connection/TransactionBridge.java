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

import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.SolaceFault;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_COMPLETED;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_COMPLETED;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;

/**
 * Submits a command to a transaction owner and waits for its result. The owner is the inbound
 * endpoint's command queue of an inbound owned transaction or the TransactionExecutor of a connector owned one.
 * Every operation that runs inside a transaction goes through this one helper, so the fault mapping is the same
 * on both sides of the bridge.
 */
public final class TransactionBridge {

    private TransactionBridge() {
    }

    /**
     * faultName and faultCode are the calling operation's own generic fault, raised when the command body failed
     * with anything but a SolaceFault.
     */
    public static <T> T submitAndWait(Executor owner, TransactionCommand<T> command, String opName, String faultName,
                                      int faultCode, MessageContext messageContext) throws SolaceFault {
        try {
            owner.execute(command);
        } catch (RejectedExecutionException e) {
            // A closed queue or a shut down executor.
            throw new SolaceFault(FAULT_TRANSACTION_COMPLETED, ERROR_CODE_TRANSACTION_COMPLETED,
                    "the transaction has ended; " + opName + " was not run");
        }
        try {
            return command.get(Math.max(1, command.deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            command.cancelledByCaller = true;
            command.cancel(false);
            if (command.started) {
                // The owner sees isCancelled() after run() and marks the transaction uncertain.
                throw new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN, opName
                        + " was cut off while running; its outcome is unknown and the transaction will be rolled back",
                        null, true);
            }
            // EXPIRED: the command never executes.
            throw new SolaceFault(FAULT_DEADLINE, ERROR_CODE_DEADLINE,
                    opName + " did not start within ackTimeout; it will never run", null, false);
        } catch (CancellationException e) {
            // Cancelled by the owner. The reason was stored through accept(String) before the cancel.
            if ("DEADLINE".equals(command.ownerReason)) {
                throw new SolaceFault(FAULT_DEADLINE, ERROR_CODE_DEADLINE,
                        opName + " did not start within ackTimeout; it will never run", null, false);
            }
            // TRANSACTION_COMPLETED, or no reason at all from a closed executor.
            throw new SolaceFault(FAULT_TRANSACTION_COMPLETED, ERROR_CODE_TRANSACTION_COMPLETED,
                    "the transaction ended before " + opName + " ran");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            command.cancelledByCaller = true;
            command.cancel(false);
            throw command.started
                    ? new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN, opName
                            + " was interrupted while running; its outcome is unknown and the transaction will be rolled back",
                            e, true)
                    : new SolaceFault(FAULT_TRANSACTION_COMPLETED, ERROR_CODE_TRANSACTION_COMPLETED,
                            opName + " was interrupted before it ran; it will never run", e, false);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SolaceFault) {
                throw (SolaceFault) cause;
            }
            // The operation's own fault, never a publish fault for a poll.
            throw new SolaceFault(faultName, faultCode, opName + " failed: " + cause.getMessage(), cause, false);
        }
    }
}
