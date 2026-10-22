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
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The session event handler of every connector session. DOWN_ERROR marks the owning session
 * failed so its next user rebuilds it. RECONNECTING refreshes the OAuth2 token from the file, as the inbound does.
 * It also answers the reconnect question of JCSMP through a guard consumer registered at connect. A session whose
 * holder is failed or retired refuses its own reconnect. A session of an orphaned logical connection refuses
 * it too and retires the connection.
 */
public final class PooledSessionEventHandler implements SessionEventHandler, JCSMPReconnectEventHandler {

    private static final Log log = LogFactory.getLog(PooledSessionEventHandler.class);

    private final SolaceLogicalConnection lc;
    private final String sessionId;
    private final SolaceConnection pooled;
    private final RequestReplySession requestReply;
    private final PollSession poll;
    // The session this handler was created for. The owner binds it right after createSession returns.
    private volatile JCSMPSession session;
    // Never started and without a subscription. It only carries this handler to JCSMP.
    private volatile XMLMessageConsumer guard;
    // Set by the holder when this session is replaced, before the old close is queued and before the publication.
    // A retired handler changes no holder state.
    volatile boolean retired;

    public PooledSessionEventHandler(SolaceConnection pooled) {
        this.lc = pooled.lc;
        this.sessionId = pooled.connectionId;
        this.pooled = pooled;
        this.requestReply = null;
        this.poll = null;
    }

    public PooledSessionEventHandler(RequestReplySession requestReply) {
        this.lc = requestReply.lc;
        this.sessionId = requestReply.lc.name + "-rr";
        this.pooled = null;
        this.requestReply = requestReply;
        this.poll = null;
    }

    public PooledSessionEventHandler(PollSession poll) {
        this.lc = poll.lc;
        this.sessionId = poll.lc.name + "-poll";
        this.pooled = null;
        this.requestReply = null;
        this.poll = poll;
    }

    void bind(JCSMPSession createdSession) {
        this.session = createdSession;
    }

    /** After the session connected. The guard is never started and no subscription is added through it. */
    void registerGuard() throws JCSMPException {
        guard = session.getMessageConsumer(this, (XMLMessageListener) null);
    }

    XMLMessageConsumer guard() {
        return guard;
    }

    void clearGuard() {
        guard = null;
    }

    void retire() {
        retired = true;
    }

    // One transition under the holder's monitor classifies and records the failure: a retired handler changes
    // nothing, the installed session's handler fails the holder, a reserved candidate's handler records it on the
    // reservation for the publication to read.
    private void reportDown() {
        if (pooled != null) {
            pooled.reportDown(this);
        } else if (requestReply != null) {
            lc.reportReplyDown(this);
        } else {
            lc.reportPollDown(this);
        }
    }

    // The failed flag of the holder this handler was built for.
    private boolean holderFailed() {
        if (pooled != null) {
            return pooled.failed;
        }
        if (requestReply != null) {
            return requestReply.failed;
        }
        return poll.failed;
    }

    @Override
    public boolean preReconnect() {
        if (retired) {
            log.warn("SolaceConnector [" + lc.name + "] session " + sessionId
                    + " reconnect refused: the session is failed or retired, it stays down for the closer");
            return false;
        }
        if (lc.orphaned()) {
            // The library was replaced, so no caller can reach this connection again.
            lc.retireOrphan();
            return false;
        }
        boolean allowed = !holderFailed();
        if (allowed) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceConnector [" + lc.name + "] session " + sessionId + " reconnect allowed");
            }
        } else {
            log.warn("SolaceConnector [" + lc.name + "] session " + sessionId
                    + " reconnect refused: the session is failed or retired, it stays down for the closer");
        }
        return allowed;
    }

    @Override
    public void postReconnect() {
        if (log.isDebugEnabled()) {
            log.debug("SolaceConnector [" + lc.name + "] session " + sessionId + " reconnect completed");
        }
    }

    @Override
    public void handleEvent(SessionEventArgs event) {
        SessionEvent sessionEvent = event.getEvent();
        if (retired) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceConnector [" + lc.name + "] session " + sessionId + " event " + sessionEvent
                        + " after its retirement ignored: " + event.getInfo());
            }
            return;
        }
        if (sessionEvent == SessionEvent.RECONNECTING) {
            JCSMPSession reconnecting = session;
            if (lc.tokenSource != null && reconnecting != null) {
                try {
                    reconnecting.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, lc.tokenSource.current());
                } catch (JCSMPException e) {
                    log.warn("SolaceConnector [" + lc.name + "] could not update the OAuth2 token on the reconnecting session",
                            e);
                }
            }
            log.warn("SolaceConnector [" + lc.name + "] session " + sessionId + " reconnecting: " + event.getInfo());
        } else if (sessionEvent == SessionEvent.RECONNECTED) {
            log.info("SolaceConnector [" + lc.name + "] session " + sessionId + " reconnected");
        } else if (sessionEvent == SessionEvent.DOWN_ERROR) {
            reportDown();
            log.error("SolaceConnector [" + lc.name + "] session " + sessionId + " DOWN: " + event.getInfo()
                    + ", responseCode=" + event.getResponseCode()
                    + ", exception=" + event.getException());
        } else if (sessionEvent == SessionEvent.SUBSCRIPTION_ERROR) {
            log.error("Solace subscription error: " + event.getInfo());
        } else {
            log.info("Solace session event: " + sessionEvent + " - " + event.getInfo());
        }
    }
}
