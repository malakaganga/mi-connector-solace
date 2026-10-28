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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_MESSAGE_LIMIT;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_MESSAGE_LIMIT;

/**
 * The admission protocol of a transacted poll: one outstanding reservation, a release that checks its token, and a
 * consumption that keeps the reservation for the life of the transaction.
 */
public class PollAdmissionTest {

    @Test
    public void secondReservationFaultsWhileTheFirstIsOutstanding() throws SolaceFault {
        PollAdmission admission = new PollAdmission("tx-1");
        PollAdmission.Token first = admission.reserve();
        assertNotNull(first);
        assertTrue(admission.isReserved());
        SolaceFault fault = expectLimit(admission);
        assertTrue(fault.getMessage().contains("has a poll outstanding"));
    }

    @Test
    public void emptyReceiveReleasesAndAdmitsTheNextPoll() throws SolaceFault {
        PollAdmission admission = new PollAdmission("tx-2");
        PollAdmission.Token first = admission.reserve();
        admission.release(first);
        assertFalse(admission.isReserved());
        PollAdmission.Token second = admission.reserve();
        assertNotNull(second);
        assertTrue(admission.isReserved());
    }

    @Test
    public void aLateReleaseWithAnOldTokenDoesNotReleaseTheNextReservation() throws SolaceFault {
        PollAdmission admission = new PollAdmission("tx-3");
        PollAdmission.Token first = admission.reserve();
        admission.release(first);
        PollAdmission.Token second = admission.reserve();
        admission.release(first);
        assertTrue(admission.isReserved());
        admission.release(second);
        assertFalse(admission.isReserved());
    }

    @Test
    public void consumptionKeepsTheReservationForLife() throws SolaceFault {
        PollAdmission admission = new PollAdmission("tx-4");
        PollAdmission.Token token = admission.reserve();
        admission.recordConsumption();
        assertTrue(admission.hasConsumed());
        admission.release(token);
        assertTrue(admission.isReserved());
        SolaceFault fault = expectLimit(admission);
        assertTrue(fault.getMessage().contains("already consumed a message"));
    }

    private static SolaceFault expectLimit(PollAdmission admission) {
        try {
            admission.reserve();
        } catch (SolaceFault fault) {
            assertEquals(FAULT_TRANSACTION_MESSAGE_LIMIT, fault.name);
            assertEquals(ERROR_CODE_TRANSACTION_MESSAGE_LIMIT, fault.code);
            return fault;
        }
        fail("expected TRANSACTION_MESSAGE_LIMIT");
        return null;
    }
}
