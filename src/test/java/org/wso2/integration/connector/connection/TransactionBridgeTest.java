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

import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;

import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_DEADLINE;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_COMPLETED;

/**
 * The two faults that prove a transacted poll command never ran, the ones that release its reservation. A refused
 * submission leaves the command untouched. A queued command that the caller gives up on is cancelled before it
 * starts and faults DEADLINE.
 */
public class TransactionBridgeTest {

    @Test
    public void aRefusedSubmissionLeavesTheCommandUntouched() {
        TransactionCommand<String> command = new TransactionCommand<>(() -> "ran", 1000L);
        try {
            TransactionBridge.submitAndWait(task -> {
                throw new RejectedExecutionException("TRANSACTION_COMPLETED");
            }, command, "poll", FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, null);
            fail("expected TRANSACTION_COMPLETED");
        } catch (SolaceFault fault) {
            assertEquals(FAULT_TRANSACTION_COMPLETED, fault.name);
            assertFalse(command.isCancelled());
            assertFalse(command.isDone());
        }
    }

    @Test
    public void aQueuedCommandPastItsDeadlineNeverRuns() {
        TransactionCommand<String> command = new TransactionCommand<>(() -> "ran", 50L);
        try {
            // The owner never takes the command, so the caller expires while it is still queued.
            TransactionBridge.submitAndWait(task -> { }, command, "poll", FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                    null);
            fail("expected DEADLINE");
        } catch (SolaceFault fault) {
            assertEquals(FAULT_DEADLINE, fault.name);
            assertFalse(fault.outcomeUnknown);
            assertTrue(command.isCancelled());
            // A late run by the owner does nothing.
            command.run();
            assertTrue(command.isCancelled());
        }
    }
}
