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

import org.wso2.integration.connector.SolaceFault;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_TRANSACTION_MESSAGE_LIMIT;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_TRANSACTION_MESSAGE_LIMIT;

/**
 * The consuming poll admission of one connector owned transaction. The transaction holds at most one outstanding
 * consuming poll, on any queue. A poll reserves before its command is submitted and gets a token. The token releases
 * the reservation after a confirmed empty receive, or after proof that the command never ran. A received message
 * records consumption on the owner thread before the result is exposed, and the reservation then stays for the life
 * of the transaction, so every later poll is refused at once. Every release checks the token, so a late cleanup never
 * releases a later poll's reservation. Transaction completion disposes of the entry and of this state with it.
 */
public final class PollAdmission {

    /** The identity of one reservation. */
    public static final class Token {

        private Token() {
        }
    }

    private final String txId;
    private Token outstanding;
    private boolean consumed;

    PollAdmission(String txId) {
        this.txId = txId;
    }

    /**
     * Any thread, before the poll command is submitted. Faults TRANSACTION_MESSAGE_LIMIT at once when the
     * transaction already consumed a message or holds an outstanding poll. Nothing waits here.
     */
    public synchronized Token reserve() throws SolaceFault {
        if (consumed) {
            throw new SolaceFault(FAULT_TRANSACTION_MESSAGE_LIMIT, ERROR_CODE_TRANSACTION_MESSAGE_LIMIT,
                    "transaction " + txId + " already consumed a message; one consumed message per transaction,"
                            + " commit or roll back before polling again");
        }
        if (outstanding != null) {
            throw new SolaceFault(FAULT_TRANSACTION_MESSAGE_LIMIT, ERROR_CODE_TRANSACTION_MESSAGE_LIMIT,
                    "transaction " + txId + " has a poll outstanding; one consuming poll at a time and one consumed"
                            + " message per transaction");
        }
        outstanding = new Token();
        return outstanding;
    }

    /**
     * Owner thread, after the receive returned a message and before the result is exposed. Consumption is a fact
     * of the transaction whatever token the poll holds, so it is recorded unconditionally.
     */
    public synchronized void recordConsumption() {
        consumed = true;
    }

    /**
     * Releases the reservation the token owns. A token that is not the outstanding one releases nothing, and a
     * transaction that consumed keeps its reservation for life.
     */
    public synchronized void release(Token token) {
        if (outstanding == token && !consumed) {
            outstanding = null;
        }
    }

    /** For diagnostics and tests. */
    public synchronized boolean isReserved() {
        return outstanding != null;
    }

    /** For diagnostics and tests. */
    public synchronized boolean hasConsumed() {
        return consumed;
    }
}
