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

/**
 * One poll call waiting for its turn on a flow owner. The mutable fields are guarded by the request's
 * own monitor, which the owner and the caller share for the one atomic hand off.
 */
public final class PollRequest {

    public static final int QUEUED = 0;
    public static final int DONE = 1;
    public static final int EXPIRED = 2;

    public final long deadline;
    public final String ackMode;
    // The start of the poll.requestToResult measurement.
    public final long createdNanos = System.nanoTime();
    public int state = QUEUED;
    public BytesXMLMessage result;
    public PollLease lease;

    public PollRequest(long deadline, String ackMode) {
        this.deadline = deadline;
        this.ackMode = ackMode;
    }
}
