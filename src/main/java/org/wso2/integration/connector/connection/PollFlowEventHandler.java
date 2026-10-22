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

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The flow event handler of a poll flow. A FLOW_DOWN or a FLOW_RECONNECTING invalidates the owner and
 * submits its close, every other event is logged at DEBUG. It runs on the JCSMP Context thread and only submits.
 */
public final class PollFlowEventHandler implements FlowEventHandler {

    private static final Log log = LogFactory.getLog(PollFlowEventHandler.class);

    private final PollFlowOwner owner;

    PollFlowEventHandler(PollFlowOwner owner) {
        this.owner = owner;
    }

    @Override
    public void handleEvent(Object source, FlowEventArgs event) {
        FlowEvent flowEvent = event.getEvent();
        if (flowEvent == FlowEvent.FLOW_DOWN) {
            // Invalidated before the close is queued, under the owner's state monitor that the hand off also holds,
            // so a message the owner receives meanwhile is never exposed.
            owner.invalidate();
            owner.submitClose("flow down");
            return;
        }
        if (flowEvent == FlowEvent.FLOW_RECONNECTING) {
            // The flow closes with its buffer, so a message JCSMP buffered before the rebind is never handed to a
            // poll beside the broker's own redelivery. The inbound endpoint's rule for a reconnecting flow, applied to the poll owner.
            owner.invalidate();
            owner.submitClose("flow reconnecting");
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("SolaceConnector [" + owner.lc.name + "] poll flow " + owner.queue + " event " + flowEvent
                    + ": " + event.getInfo());
        }
    }
}
