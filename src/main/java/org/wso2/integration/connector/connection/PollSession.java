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

import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.SolaceFault;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_POLL_FLOW_LIMIT;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_POLL_FLOW_LIMIT;
import static org.wso2.integration.connector.constants.SolaceConstants.REQUIRE_OUTCOMES_TRUE;

/**
 * The one dedicated poll session of a logical connection and its cache of poll flow owners.
 * Created lazily and connected by SolaceLogicalConnection.pollSession(op) outside the dedicated lock, under
 * the poll operation's record and one physical attempt at a time. Rebuilt on the next use after DOWN_ERROR.
 */
public final class PollSession {

    private static final Log log = LogFactory.getLog(PollSession.class);

    final SolaceLogicalConnection lc;
    final int generation;
    // Set by connect(). DOWN_ERROR sets failed.
    volatile JCSMPSession session;
    // The handler of the session, with the reconnect guard registered at connect.
    volatile PooledSessionEventHandler handler;
    volatile boolean failed = false;
    volatile boolean retired = false;
    private final AtomicBoolean retiredOnce = new AtomicBoolean();
    // Completes when the retirement's close finished.
    private final CompletableFuture<Void> retiredClose = new CompletableFuture<>();
    // Access order, least recently used first. Guarded by synchronized (flows), never held across a broker call.
    final Map<String, PollFlowOwner> flows = new LinkedHashMap<>(16, 0.75f, true);
    // Per key creation reservations, guarded by flows. They count toward maxPollFlows.
    private final Map<String, CountDownLatch> creating = new HashMap<>();
    // Negotiated once per session. A TRUE mismatch is reported by ownerFor as INVALID_CONFIG 700103 at every poll.
    boolean failedSupported;
    boolean rejectedSupported;
    private volatile String mismatch;

    PollSession(SolaceLogicalConnection lc) {
        this.lc = lc;
        // The creation reservation serialises the creation attempts, so one attempt at a time reads and
        // writes the number.
        this.generation = ++lc.pollGeneration;
        // The constructor ends with the attachment. The creation attempt owns this candidate until publication.
        lc.attach(this);
    }

    /** The creation attempt: the session and its handler, not connected. The logical connection connects it. */
    JCSMPSession createSession() throws JCSMPException {
        PooledSessionEventHandler sessionHandler = new PooledSessionEventHandler(this);
        JCSMPSession created = lc.createSession("poll", sessionHandler);
        sessionHandler.bind(created);
        handler = sessionHandler;
        session = created;
        return created;
    }

    /** The 1.6 algorithm on the connector side, after the connect. A TRUE mismatch is reported by the first poll. */
    void negotiate(JCSMPSession connected) throws JCSMPException {
        // ArrayList<Boolean> [accepted, failed, rejected], or null on a broker without the capability.
        Object capability = connected.getCapability(CapabilityType.SUPPORTED_MESSAGE_SETTLEMENT_CAPABILITIES);
        List<?> outcomes = capability instanceof List ? (List<?>) capability : null;
        failedSupported = outcomes != null && outcomes.size() > 1 && Boolean.TRUE.equals(outcomes.get(1));
        rejectedSupported = outcomes != null && outcomes.size() > 2 && Boolean.TRUE.equals(outcomes.get(2));
        List<String> missing = new ArrayList<>();
        if (!failedSupported) {
            missing.add("FAILED");
        }
        if (!rejectedSupported) {
            missing.add("REJECTED");
        }
        if (missing.isEmpty()) {
            return;
        }
        if (REQUIRE_OUTCOMES_TRUE.equalsIgnoreCase(lc.settings.requireSettlementOutcomes)) {
            mismatch = "the broker reports no support for settlement outcome(s) " + missing
                    + " and requireSettlementOutcomes=TRUE";
        } else {
            log.warn("SolaceConnector [" + lc.name + "] the broker reports no support for settlement outcome(s) " + missing
                    + "; a polled message that needs one is retained and its flow recycled (requireSettlementOutcomes=AUTO)");
        }
    }

    /** Operation thread. Returns an owner that has counted this caller as busy. */
    public PollFlowOwner ownerFor(String queue, String selector) throws SolaceFault, JCSMPException {
        String key = queue + "\u0000" + (selector == null ? "" : selector.trim());
        // Negotiated at the session's connect.
        if (mismatch != null) {
            throw new SolaceFault(FAULT_INVALID_CONFIG, ERROR_CODE_INVALID_CONFIG, mismatch);
        }
        while (true) {
            CountDownLatch wait = null;
            PollFlowOwner create = null;
            synchronized (flows) {
                if (retired) {
                    throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR, "poll session retired");
                }
                // The access order touch.
                PollFlowOwner cached = flows.get(key);
                // accept() is false when closing. True increments busy.
                if (cached != null && cached.accept()) {
                    return cached;
                }
                // A closing owner is dropped from the map. Its own closeNow does the same.
                if (cached != null) {
                    flows.remove(key, cached);
                }
                wait = creating.get(key);
                if (wait == null) {
                    // Reservations count toward the capacity: two first callers on different keys cannot both pass
                    // a limit of one.
                    if (flows.size() + creating.size() >= lc.settings.maxPollFlows) {
                        PollFlowOwner victim = null;
                        // Idle means no busy request, no live lease and nothing in the inbox. closing is set in the
                        // same step.
                        for (PollFlowOwner candidate : flows.values()) {
                            if (candidate.beginEvictionIfIdle()) {
                                victim = candidate;
                                break;
                            }
                        }
                        if (victim == null) {
                            throw new SolaceFault(FAULT_POLL_FLOW_LIMIT, ERROR_CODE_POLL_FLOW_LIMIT,
                                    "maxPollFlows=" + lc.settings.maxPollFlows + " flows are cached on connection '"
                                            + lc.name + "' and none is idle");
                        }
                        flows.values().remove(victim);
                        victim.submitClose("evicted");
                    }
                    // The owner is attached to lc in its constructor.
                    create = new PollFlowOwner(this, key, queue, selector);
                    creating.put(key, create.ready);
                }
            }
            if (wait != null) {
                // Another caller is binding this key: wait for its result, then retry the lookup. Nothing was
                // counted yet.
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                            "interrupted while another caller binds the poll flow");
                }
                continue;
            }
            boolean published = false;
            try {
                // Outside the flows lock: a slow bind on one queue does not stall other flows.
                create.bind();
                synchronized (flows) {
                    creating.remove(key);
                    // Retirement rechecked at publication: a bind released after retire() never publishes. The
                    // first caller's reservation is taken before the thread starts, so accept() is true here. Then
                    // the thread, then the map entry. A Thread.start() failure leaves published false, so the
                    // finally discards the owner that never entered the map.
                    if (!retired) {
                        create.accept();
                        create.startThread();
                        flows.put(key, create);
                        published = true;
                    }
                }
            } finally {
                // A failed bind or a lost publication: close the partial flow and detach the owner. It was never
                // published and never started.
                if (!published) {
                    synchronized (flows) {
                        creating.remove(key);
                    }
                    create.discard();
                }
                create.ready.countDown();
            }
            if (!published) {
                throw new SolaceFault(FAULT_POLL_ERROR, ERROR_CODE_POLL_ERROR,
                        "poll session retired during the flow bind");
            }
            return create;
        }
    }

    /**
     * lc.close(), a replacement after DOWN_ERROR, local entry undeploy, or the cleanup of an unpublished
     * candidate. Idempotent. The returned future completes when the session close finished.
     */
    CompletableFuture<Void> retire() {
        if (!retiredOnce.compareAndSet(false, true)) {
            return retiredClose;
        }
        List<PollFlowOwner> all;
        // Creations in progress see retired at their publication and discard themselves.
        synchronized (flows) {
            retired = true;
            all = new ArrayList<>(flows.values());
            flows.clear();
        }
        // Each owner terminalises its live leases and detaches. A late settle finds the retired session and
        // reports STALE_DELIVERY.
        for (PollFlowOwner owner : all) {
            owner.submitClose("session retired");
        }
        // Null for a candidate whose createSession failed.
        JCSMPSession retiring = session;
        CompletableFuture<Void> closed = retiring != null ? lc.closeTracked(retiring)
                : CompletableFuture.completedFuture(null);
        closed.whenComplete((ignored, error) -> {
            // Exactly once, after this owner's close finished.
            lc.detach(this);
            retiredClose.complete(null);
        });
        return retiredClose;
    }
}
