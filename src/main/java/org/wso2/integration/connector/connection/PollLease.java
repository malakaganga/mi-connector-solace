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

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.util.concurrent.ScheduledFuture;

/**
 * One delivery handed out by a poll flow owner. The object stashed under
 * solace.poll.message is this lease, so a caller's timeout can never orphan a lease. An owner may hold several
 * live leases at once, one per message context polling the same flow, and each is removed by its own settle or
 * expiry.
 */
public final class PollLease {

    public static final int OUTSTANDING = 0;
    public static final int SETTLED = 1;
    public static final int EXPIRED = 2;
    /** Dropped by a flow close or a session retire. The broker redelivers it and a later settle reports STALE_DELIVERY. */
    public static final int STALE = 3;

    public final PollFlowOwner owner;
    public final int generation;
    public final BytesXMLMessage message;
    public final long deadline;
    // Written on the owner thread only. settle() and the poll guard read it.
    public volatile int state = OUTSTANDING;
    // Owner thread only.
    ScheduledFuture<?> timer;

    PollLease(PollFlowOwner owner, int generation, BytesXMLMessage message, long deadline) {
        this.owner = owner;
        this.generation = generation;
        this.message = message;
        this.deadline = deadline;
    }

    public static String stateName(int state) {
        switch (state) {
            case OUTSTANDING:
                return "OUTSTANDING";
            case SETTLED:
                return "SETTLED";
            case EXPIRED:
                return "EXPIRED";
            case STALE:
                return "STALE";
            default:
                return String.valueOf(state);
        }
    }
}
