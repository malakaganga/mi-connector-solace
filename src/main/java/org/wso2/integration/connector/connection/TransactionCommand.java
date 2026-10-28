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

import org.wso2.integration.connector.hooks.Hooks;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * One command of a transaction. The command contract is shared with the inbound
 * endpoint's owner loop: a FutureTask that is Delayed, so the owner can enforce the caller's deadline, and a
 * Consumer of the owner's cancellation reason, so the caller never infers the reason from the clock. All three
 * are JDK types, so the contract crosses the library boundary.
 */
public final class TransactionCommand<T> extends FutureTask<T> implements Delayed, Consumer<String> {

    // The caller's ackTimeout from the moment the command was created.
    final long deadline;
    // Set in run() before FutureTask.run(), so the caller knows whether the body may have started.
    volatile boolean started;
    volatile boolean cancelledByCaller;
    // The owner's cancellation reason, stored through accept(String) before the owner's cancel.
    volatile String ownerReason;
    // The hook point a test build can hold this command at, right after its execution claim. Null for no hook.
    private final String claimHook;

    public TransactionCommand(Callable<T> body, long ackTimeoutMillis) {
        this(body, ackTimeoutMillis, null);
    }

    /** The absolute deadline is computed with checked arithmetic, so a huge budget fails here and not in the owner. */
    public TransactionCommand(Callable<T> body, long ackTimeoutMillis, String claimHook) {
        super(body);
        this.deadline = Math.addExact(System.currentTimeMillis(), ackTimeoutMillis);
        this.claimHook = claimHook;
    }

    @Override
    public void run() {
        started = true;
        if (claimHook != null) {
            Hooks.get().point(claimHook, this);
        }
        super.run();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /** Required by Delayed. The owners never order commands by it. */
    @Override
    public int compareTo(Delayed other) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public void accept(String reason) {
        ownerReason = reason;
    }
}
