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

package org.wso2.integration.connector.models;

import java.util.Map;

/**
 * Holds optional message properties for Solace messages.
 */
public class SolaceMessageProperties {

    private String correlationId;
    private Integer priority;
    private Long timeToLive;
    private String applicationMessageId;
    private String applicationMessageType;
    private String senderId;
    private String replyToDestinationType;
    private String replyToDestinationName;
    private Map<String, Object> userProperties;
    private Boolean dmqEligible;
    private Boolean elidingEligible;
    private Long expiration;

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(Long timeToLive) {
        this.timeToLive = timeToLive;
    }

    public String getApplicationMessageId() {
        return applicationMessageId;
    }

    public void setApplicationMessageId(String applicationMessageId) {
        this.applicationMessageId = applicationMessageId;
    }

    public String getApplicationMessageType() {
        return applicationMessageType;
    }

    public void setApplicationMessageType(String applicationMessageType) {
        this.applicationMessageType = applicationMessageType;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getReplyToDestinationType() {
        return replyToDestinationType;
    }

    public void setReplyToDestinationType(String replyToDestinationType) {
        this.replyToDestinationType = replyToDestinationType;
    }

    public String getReplyToDestinationName() {
        return replyToDestinationName;
    }

    public void setReplyToDestinationName(String replyToDestinationName) {
        this.replyToDestinationName = replyToDestinationName;
    }

    public Map<String, Object> getUserProperties() {
        return userProperties;
    }

    public void setUserProperties(Map<String, Object> userProperties) {
        this.userProperties = userProperties;
    }

    public Boolean getDmqEligible() {
        return dmqEligible;
    }

    public void setDmqEligible(Boolean dmqEligible) {
        this.dmqEligible = dmqEligible;
    }

    public Boolean getElidingEligible() {
        return elidingEligible;
    }

    public void setElidingEligible(Boolean elidingEligible) {
        this.elidingEligible = elidingEligible;
    }

    public Long getExpiration() {
        return expiration;
    }

    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }
}
