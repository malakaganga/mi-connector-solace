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

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactionResultUnknownException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_ROLLED_BACK;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_UNKNOWN;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_ROLLED_BACK;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_UNKNOWN;

/**
 * The owner of one connector owned transaction. It is the connector's copy of the inbound
 * endpoint's owner loop: one daemon thread, one queue of commands, and the same procedure per command. Every
 * access to the transacted session, its producer and its flows happens on this thread. The completion command
 * runs here too, decides commit or rollback from the transaction's state, and releases the pinned holder.
 */
public final class TransactionExecutor implements Executor {

    private static final Log log = LogFactory.getLog(TransactionExecutor.class);

    final SolaceTransactionRegistry.Entry entry;
    final String txId;
    final SolaceLogicalConnection lc;
    final Thread thread;
    final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    // The command inside run(), for diagnostics.
    volatile Runnable running;
    // Set by the completion command. The loop exits after it.
    volatile boolean finished;

    TransactionExecutor(SolaceTransactionRegistry.Entry entry, String txId, SolaceLogicalConnection lc) {
        this.entry = entry;
        this.txId = txId;
        this.lc = lc;
        this.thread = new Thread(this::loop, "solace-conn-" + lc.name + "-tx-" + txId);
        thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    /** Any thread. The same execute and close monitor rule as the inbound's CommandQueue. */
    @Override
    public void execute(Runnable command) {
        if (Thread.currentThread() == thread) {
            // The synchronous leg runs inline, through the same admission and execution contract as a queued command.
            if (entry.terminal) {
                throw new RejectedExecutionException("TRANSACTION_COMPLETED");
            }
            runCommand(command);
            return;
        }
        synchronized (queue) {
            // closeAdmission() takes the same monitor. A command is either queued before the close and cancelled
            // by it, or refused.
            if (entry.terminal) {
                throw new RejectedExecutionException("TRANSACTION_COMPLETED");
            }
            queue.offer(command);
        }
    }

    /** Owner thread. The same procedure as the inbound's owner loop, with the terminal flag in place of the handle. */
    void runCommand(Object item) {
        if (!(item instanceof Runnable)) {
            return;
        }
        Runnable command = (Runnable) item;
        Future<?> future = command instanceof Future ? (Future<?>) command : null;
        if (future != null && future.isCancelled()) {
            // CANCELLED by the caller.
            return;
        }
        if (future != null && command instanceof Delayed
                && ((Delayed) command).getDelay(TimeUnit.MILLISECONDS) <= 0) {
            // EXPIRED: the owner enforces the deadline.
            reason(command, "DEADLINE");
            future.cancel(false);
            return;
        }
        if (entry.terminal) {
            // TRANSACTION_COMPLETED to the caller.
            if (future != null) {
                reason(command, "TRANSACTION_COMPLETED");
                future.cancel(false);
            }
            return;
        }
        running = command;
        try {
            command.run();
        } finally {
            running = null;
        }
        if (future != null && future.isCancelled()) {
            // A cancel that lands between the check above and FutureTask.run() ends here too, a spurious rollback in
            // the safe direction.
            entry.uncertain = true;
            log.error("solace transaction " + txId
                    + ": a command was cut off while running; the transaction is uncertain and the completion will roll back");
        }
    }

    /** The thread body: take, runCommand, until the completion command has run. */
    private void loop() {
        Object item;
        while (!finished) {
            try {
                item = queue.take();
            } catch (InterruptedException e) {
                // Nobody interrupts the owner. The flag carries no signal here, so the loop keeps serving, as
                // the inbound's owner loop does.
                continue;
            }
            runCommand(item);
        }
    }

    /**
     * Owner thread, inside the completion command, before commit or rollback. Atomic with the monitor of execute():
     * every operation still queued completes with TRANSACTION_COMPLETED to its caller, promptly, reason first.
     */
    void closeAdmission() {
        synchronized (queue) {
            entry.terminal = true;
            Object queued;
            while ((queued = queue.poll()) != null) {
                if (queued instanceof Future) {
                    reason(queued, "TRANSACTION_COMPLETED");
                    ((Future<?>) queued).cancel(false);
                }
            }
        }
    }

    /** The owner stores its cancellation reason before cancel(false), so the caller reads it with the cancellation. */
    @SuppressWarnings("unchecked")
    static void reason(Object command, String cancellationReason) {
        if (command instanceof Consumer) {
            ((Consumer<String>) command).accept(cancellationReason);
        }
    }

    /** The completion command of this transaction, one for commit and rollback. The owner decides. */
    public CompletionCommand completionCommand(boolean wantCommit) {
        return new CompletionCommand(() -> complete(wantCommit));
    }

    /**
     * Owner thread. Never cancelled by its caller: the caller's TX_COMPLETION_WAIT_MS bounds its wait only, and the
     * owner always runs the completion, the pool return and the entry's detach.
     */
    Void complete(boolean wantCommit) throws SolaceFault {
        // Terminal. Queued commands are cancelled.
        closeAdmission();
        // Decided here, after every earlier command has finished on this thread. A command cut off while running
        // has already set uncertain.
        boolean commit = wantCommit && !entry.uncertain;
        if (wantCommit && entry.uncertain) {
            log.error("solace.commit: txId=" + txId + " refused: the transaction is uncertain; rolling back");
        }
        SolaceFault result = null;
        boolean healthy = true;
        try {
            try {
                if (commit) {
                    entry.connection.commitTransaction();
                } else {
                    entry.connection.rollbackTransaction();
                }
            } catch (RollbackException e) {
                // Commit only. The broker rolled the transaction back, a known outcome, the session is kept.
                log.warn("solace.commit: txId=" + txId + " was rolled back by the broker: " + e.getMessage());
                result = new SolaceFault(FAULT_ROLLED_BACK, ERROR_CODE_ROLLED_BACK,
                        "the broker rolled transaction " + txId + " back; nothing was committed", e, false);
            } catch (TransactionResultUnknownException e) {
                // Commit only. The result is unknown and the session is discarded.
                log.error("solace.commit: txId=" + txId + " result unknown", e);
                healthy = false;
                result = new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                        "commit result unknown for transaction " + txId, e, true);
            } catch (JCSMPException e) {
                // Any other failure of commit or rollback. The session is discarded and the caller never sees success.
                log.error("solace." + (commit ? "commit" : "rollback") + ": txId=" + txId + " failed", e);
                healthy = false;
                result = new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN, (commit ? "commit" : "rollback")
                        + " of transaction " + txId + " failed; its result is unknown", e, true);
            }
            if (result == null && wantCommit && !commit) {
                result = new SolaceFault(FAULT_UNKNOWN, ERROR_CODE_UNKNOWN,
                        "transaction " + txId + " was uncertain and has been rolled back", null, true);
            }
        } finally {
            // The mandatory release runs on every path, an unexpected exception included.
            try {
                if (!healthy) {
                    // The holder goes back with failed=true: the pool keeps its accounting and the next borrower's
                    // ensureSession rebuilds.
                    entry.connection.failed = true;
                    lc.closeOnCloser(entry.connection.session);
                }
                // Exactly one pool release per transaction.
                entry.connection.returnToPool();
            } catch (RuntimeException e) {
                log.error("solace transaction " + txId + ": holder release failed", e);
                try {
                    entry.connection.orphan();
                } catch (RuntimeException orphanFailure) {
                    log.error("solace transaction " + txId + ": orphan cleanup failed", orphanFailure);
                }
            } finally {
                // The entry's own attachment is released whatever happened to the holder. The loop exits after this
                // command.
                lc.detach(entry);
                finished = true;
            }
        }
        if (result != null) {
            throw result;
        }
        return null;
    }

    /**
     * A FutureTask that does not implement Delayed, so runCommand's deadline check never applies to it: the
     * mandatory cleanup is exempt from expiry.
     */
    public static final class CompletionCommand extends FutureTask<Void> {

        CompletionCommand(Callable<Void> body) {
            super(body);
        }
    }
}
