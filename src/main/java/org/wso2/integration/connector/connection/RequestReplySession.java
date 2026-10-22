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
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The one dedicated request reply session of a logical connection.
 * Created by createSession() and connected by its creation attempt, one physical attempt at a time.
 */
public final class RequestReplySession {

    private static final Log log = LogFactory.getLog(RequestReplySession.class);

    final SolaceLogicalConnection lc;
    public volatile JCSMPSession session;
    // The lock covers send() only above a publisher window of 1.
    public final SessionPublisher publisher;
    public final Semaphore permits;
    // The 1.0.1 session consumer, needed by the Direct Requestor.
    private XMLMessageConsumer consumer;
    private final Object consumerLock = new Object();
    public volatile boolean failed = false;
    private final AtomicBoolean retiredOnce = new AtomicBoolean();
    // Completes when the retirement's close finished.
    private final CompletableFuture<Void> retired = new CompletableFuture<>();
    private final String connectionId;
    // The handler of the session, with the reconnect guard registered at connect.
    volatile PooledSessionEventHandler handler;

    RequestReplySession(SolaceLogicalConnection lc) {
        this.lc = lc;
        this.connectionId = lc.name + "-rr";
        this.publisher = new SessionPublisher(lc, connectionId);
        this.permits = new Semaphore(lc.settings.maxConcurrentRequests, true);
        // The constructor ends with the attachment. The creation attempt owns this candidate until publication.
        lc.attach(this);
    }

    JCSMPSession createSession() throws JCSMPException {
        PooledSessionEventHandler sessionHandler = new PooledSessionEventHandler(this);
        JCSMPSession created = lc.createSession("rr", sessionHandler);
        sessionHandler.bind(created);
        handler = sessionHandler;
        session = created;
        return created;
    }

    /** After the connect: the producer of the connected session, and the publisher rebuilt with it. */
    void installProducer() throws JCSMPException {
        publisher.swap(session, session.getMessageProducer(publisher.eventHandler()));
    }

    /**
     * Idempotent. The cleanup owner of an unpublished candidate and of a replaced or closed session. Outstanding
     * requests fault on their next step through a closed flow or a failed future. The returned future completes
     * when the close finished, and the attachment is released only then.
     */
    CompletableFuture<Void> retire() {
        if (!retiredOnce.compareAndSet(false, true)) {
            return retired;
        }
        failed = true;
        publisher.failPending("request-reply session retired");
        JCSMPSession retiring = session;
        CompletableFuture<Void> closed = retiring != null ? lc.closeTracked(retiring)
                : CompletableFuture.completedFuture(null);
        closed.whenComplete((ignored, error) -> {
            // Exactly once, after this owner's close finished.
            lc.detach(this);
            retired.complete(null);
        });
        return retired;
    }

    /**
     * Lazily creates and starts the session's synchronous consumer the first time an
     * operation needs to receive on the session — {@link #requestCachedMessages} (cached
     * messages are delivered here) or the DIRECT branch of {@link #sendRequest} (the
     * {@link Requestor} reads its reply here). Connections that only publish/poll/browse
     * never create one. Idempotent and synchronized so reuse across pooled borrows returns
     * the same started consumer.
     */
    public XMLMessageConsumer ensureConsumer() throws JCSMPException {
        synchronized (consumerLock) {
            if (consumer == null) {
                XMLMessageConsumer newConsumer = null;
                try {
                    // The guard registered at connect is the consumer to start, so the reconnect handler stays
                    // with the session. Without one, a null listener gives the synchronous consumer as today.
                    XMLMessageConsumer guard = handler.guard();
                    newConsumer = guard != null ? guard : session.getMessageConsumer((XMLMessageListener) null);
                    newConsumer.start();
                    // Publish to the field only once fully started, so a start() failure can't
                    // leave a half-initialized consumer that later calls would blindly reuse.
                    consumer = newConsumer;
                    if (log.isDebugEnabled()) {
                        log.debug("Session consumer started on connection: " + connectionId);
                    }
                } catch (JCSMPException e) {
                    // Close the partially-created consumer so it isn't leaked and the next call
                    // retries from a clean state. Don't log here — the operation layer
                    // (SolaceRequestCachedMessages / SolaceSendRequest) logs and reports this as
                    // a connector fault. Rethrow with context so that single log says where it
                    // failed, instead of duplicate-logging the same error at this hop.
                    if (newConsumer != null) {
                        try {
                            newConsumer.close();
                        } catch (Exception ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Error closing partially-created consumer on connection: "
                                        + connectionId + " — " + ignored.getMessage());
                            }
                        }
                    }
                    // The next call creates a fresh consumer.
                    handler.clearGuard();
                    throw new JCSMPException("Failed to create session consumer on connection "
                            + connectionId, e);
                }
            }
            return consumer;
        }
    }
}
