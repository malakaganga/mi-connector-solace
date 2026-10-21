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

package org.wso2.integration.connector.constants;

/**
 * Constants used by the Solace connector.
 */
public final class SolaceConstants {


    // Connector name
    public static final String CONNECTOR_NAME = "solace";

    // Connection parameters
    public static final String HOST = "host";
    public static final String VPN_NAME = "vpnName";
    public static final String USERNAME = "username";
    public static final String API_PROVIDED_USERNAME = "apiProvidedUsername";
    public static final String PASSWORD = "password";
    public static final String CLIENT_NAME = "clientName";
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    public static final String READ_TIMEOUT = "readTimeout";
    public static final String COMPRESSION_LEVEL = "compressionLevel";
    public static final String GENERATE_SEND_TIMESTAMPS = "generateSendTimestamps";
    public static final String GENERATE_RECEIVE_TIMESTAMPS = "generateReceiveTimestamps";
    public static final String GENERATE_SEQUENCE_NUMBERS = "generateSequenceNumbers";
    public static final String CALCULATE_MESSAGE_EXPIRATION = "calculateMessageExpiration";

    // Connection retry parameters
    public static final String CONNECT_RETRIES = "connectRetries";
    public static final String CONNECT_RETRIES_PER_HOST = "connectRetriesPerHost";
    public static final String RECONNECT_RETRIES = "reconnectRetries";
    public static final String RECONNECT_RETRY_WAIT = "reconnectRetryWait";

    // SSL/TLS parameters
    public static final String SSL_TRUST_STORE_PATH = "sslTrustStorePath";
    public static final String SSL_TRUST_STORE_PASSWORD = "sslTrustStorePassword";
    public static final String SSL_TRUST_STORE_FORMAT = "sslTrustStoreFormat";
    public static final String SSL_KEY_STORE_PATH = "sslKeyStorePath";
    public static final String SSL_KEY_STORE_PASSWORD = "sslKeyStorePassword";
    public static final String SSL_KEY_STORE_FORMAT = "sslKeyStoreFormat";
    public static final String SSL_KEY_PASSWORD = "sslKeyPassword";
    public static final String SSL_VALIDATE_CERTIFICATE = "sslValidateCertificate";
    public static final String SSL_VALIDATE_CERTIFICATE_DATE = "sslValidateCertificateDate";
    public static final String SSL_VALIDATE_HOSTNAME = "sslValidateHostname";

    /** Authentication scheme. BASIC | CLIENT_CERTIFICATE | OAUTH2. */
    public static final String AUTHENTICATION_SCHEME = "authenticationScheme";

    /** Optional values for AUTHENTICATION_SCHEME. */
    public static final String AUTH_SCHEME_BASIC              = "BASIC";
    public static final String AUTH_SCHEME_CLIENT_CERTIFICATE = "CLIENT_CERTIFICATE";
    public static final String AUTH_SCHEME_OAUTH2             = "OAUTH2";

    /** OAuth2 / OIDC credential parameters (used when AUTHENTICATION_SCHEME == OAUTH2). */
    public static final String OAUTH2_ACCESS_TOKEN       = "oauth2AccessToken";
    public static final String OAUTH2_ISSUER_IDENTIFIER  = "oauth2IssuerIdentifier";
    public static final String OIDC_ID_TOKEN             = "oidcIdToken";

    // Publisher acknowledgement window
    public static final String PUB_ACK_WINDOW_SIZE = "pubAckWindowSize";

    // Publish message parameters
    public static final String DESTINATION_TYPE = "destinationType";
    public static final String DESTINATION_NAME = "destinationName";
    public static final String DELIVERY_MODE = "deliveryMode";
    public static final String PRIORITY = "priority";
    public static final String TIME_TO_LIVE = "timeToLive";
    public static final String CORRELATION_ID = "correlationId";
    public static final String APPLICATION_MESSAGE_ID = "applicationMessageId";
    public static final String APPLICATION_MESSAGE_TYPE = "applicationMessageType";
    public static final String REPLY_TO_DESTINATION_TYPE = "replyToDestinationType";
    public static final String REPLY_TO_DESTINATION_NAME = "replyToDestinationName";
    public static final String SENDER_ID = "senderId";
    public static final String USER_PROPERTIES = "userProperties";
    public static final String REQUEST_TIMEOUT = "requestTimeout";
    public static final String MESSAGE_TYPE = "messageType";
    public static final String DMQ_ELIGIBLE = "dmqEligible";
    public static final String ELIDING_ELIGIBLE = "elidingEligible";
    public static final String EXPIRATION = "expiration";

    // Message type values
    public static final String MESSAGE_TYPE_TEXT = "TEXT";
    public static final String MESSAGE_TYPE_BYTES = "BYTES";
    public static final String MESSAGE_TYPE_XML = "XML";

    // Destination types
    public static final String DESTINATION_TYPE_TOPIC = "TOPIC";
    public static final String DESTINATION_TYPE_QUEUE = "QUEUE";

    // Delivery modes
    public static final String DELIVERY_MODE_DIRECT = "DIRECT";
    public static final String DELIVERY_MODE_PERSISTENT = "PERSISTENT";
    public static final String DELIVERY_MODE_NON_PERSISTENT = "NON_PERSISTENT";

    // Connection name parameter
    public static final String NAME = "name";

    // Default values
    public static final String DEFAULT_VPN_NAME = "default";
    public static final int DEFAULT_CONNECTION_TIMEOUT = 30000;
    public static final int DEFAULT_READ_TIMEOUT = 10000;
    public static final int DEFAULT_COMPRESSION_LEVEL = 0;

    // Message context properties
    public static final String SOLACE_MESSAGE_ID = "solace.messageId";
    public static final String SOLACE_DESTINATION = "solace.destination";
    public static final String SOLACE_DELIVERY_MODE = "solace.deliveryMode";
    public static final String SOLACE_PRIORITY = "solace.priority";
    public static final String SOLACE_CORRELATION_ID = "solace.correlationId";
    public static final String SOLACE_REPLY_TO = "solace.replyTo";
    public static final String SOLACE_SENDER_ID = "solace.senderId";
    public static final String SOLACE_SENDER_TIMESTAMP = "solace.senderTimestamp";
    public static final String SOLACE_RECEIVE_TIMESTAMP = "solace.receiveTimestamp";
    public static final String SOLACE_SEQUENCE_NUMBER = "solace.sequenceNumber";
    public static final String SOLACE_REDELIVERED = "solace.redelivered";
    public static final String SOLACE_DELIVERY_COUNT = "solace.deliveryCount";
    public static final String SOLACE_APPLICATION_MESSAGE_ID = "solace.applicationMessageId";
    public static final String SOLACE_APPLICATION_MESSAGE_TYPE = "solace.applicationMessageType";
    public static final String SOLACE_REPLY_RECEIVE_TIMESTAMP = "solace.reply.receiveTimestamp";

    // Inbound message context properties (set by inbound endpoint for sendReply/acknowledgeMessage)
    public static final String SOLACE_INBOUND_MESSAGE = "solace.inbound.message";
    public static final String SOLACE_INBOUND_REPLY_TO = "solace.inbound.replyTo";
    public static final String SOLACE_INBOUND_CORRELATION_ID = "solace.inbound.correlationId";
    public static final String SOLACE_INBOUND_REPLY_TO_DESTINATION_TYPE = "solace.inbound.replyToDestinationType";
    /**
     * Set to {@code Boolean.TRUE} by acknowledgeMessage / nackMessage after they settle the
     * inbound JCSMP message, signalling to the inbound endpoint that it must not settle the
     * message again post-mediation. Must match
     * {@code SolaceInboundConstants.SOLACE_INBOUND_MESSAGE_SETTLED} in the inbound module.
     */
    public static final String SOLACE_INBOUND_MESSAGE_SETTLED = "solace.inbound.message.settled";

    // NACK settlement outcomes
    public static final String OUTCOME_TYPE = "outcomeType";
    public static final String OUTCOME_FAILED = "FAILED";
    public static final String OUTCOME_REJECTED = "REJECTED";

    // Error codes
    public static final String ERROR_CODE = "ERROR_CODE";
    public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    public static final int ERROR_CODE_CONNECTION_ERROR = 700101;
    public static final int ERROR_CODE_PUBLISH_ERROR = 700102;
    public static final int ERROR_CODE_INVALID_CONFIG = 700103;
    public static final int ERROR_CODE_REQUEST_REPLY_ERROR = 700104;
    public static final int ERROR_CODE_ACK_ERROR = 700105;
    public static final int ERROR_CODE_NACK_ERROR = 700106;

    // Publish-ack (guaranteed-mode) parameters
    public static final String WAIT_FOR_ACK = "waitForAck";
    public static final String ACK_TIMEOUT = "ackTimeout";
    public static final String CONTINUE_ON_ACK_FAILURE = "continueOnAckFailure";
    public static final long DEFAULT_ACK_TIMEOUT_MS = 10000L;
    public static final int ERROR_CODE_ACK_TIMEOUT = 700107;

    // Publish-ack outcome properties (set on message context after publish)
    public static final String SOLACE_ACK_STATUS = "solace.ack.status";
    public static final String SOLACE_ACK_RECEIVED = "solace.ack.received";
    public static final String SOLACE_ACK_ERROR = "solace.ack.error";
    public static final String SOLACE_ACK_CORRELATION_KEY = "solace.ack.correlationKey";

    // Ack status values
    public static final String ACK_STATUS_ACK = "ACK";
    public static final String ACK_STATUS_NACK = "NACK";
    public static final String ACK_STATUS_TIMEOUT = "TIMEOUT";
    public static final String ACK_STATUS_NOT_APPLICABLE = "NOT_APPLICABLE";
    public static final String ACK_STATUS_NOT_REQUESTED = "NOT_REQUESTED";

    // Transaction values
    public static final String TX_CONNECTION_ID = "solace.tx.connectionId";
    public static final String TX_TIMEOUT_MILLIS = "transactionTimeoutMillis";
    public static final String DEFAULT_TX_TIMEOUT_MILLIS = "60000";
    public static final String REQUIRE_TRANSACTION = "requireTransaction";
    public static final String ACK_STATUS_TX_PENDING = "TX_PENDING";
    public static final String ACK_STATUS_TX_COMMITTED = "TX_COMMITTED";
    public static final String ACK_STATUS_TX_ROLLED_BACK = "TX_ROLLED_BACK";

    // Poll (synchronous receive from queue) parameters
    public static final String QUEUE_NAME = "queueName";
    public static final String POLL_TIMEOUT = "pollTimeout";
    public static final long DEFAULT_POLL_TIMEOUT_MS = 5000L;
    public static final int ERROR_CODE_POLL_ERROR = 700108;

    // Browse queue parameters
    public static final String MAX_MESSAGES = "maxMessages";
    public static final String BROWSE_TIMEOUT = "browseTimeout";
    public static final String SELECTOR = "selector";
    public static final int DEFAULT_BROWSE_MAX_MESSAGES = 10;
    public static final long DEFAULT_BROWSE_TIMEOUT_MS = 2000L;
    public static final int ERROR_CODE_BROWSE_ERROR = 700109;

    // Solace Cache request parameters
    public static final String CACHE_NAME = "cacheName";
    public static final String TOPIC_PATTERN = "topicPattern";
    public static final String CACHE_REQUEST_TIMEOUT = "cacheRequestTimeout";
    public static final String CACHE_MAX_MESSAGES = "cacheMaxMessages";
    public static final String CACHE_MAX_AGE_SECONDS = "cacheMaxAgeSeconds";
    public static final String CACHE_LIVE_DATA_ACTION = "liveDataAction";
    public static final String CACHE_LIVE_DATA_FULFILL = "FULFILL";
    public static final String CACHE_LIVE_DATA_FLOW_THRU = "FLOW_THRU";
    public static final String CACHE_LIVE_DATA_QUEUE = "QUEUE";
    public static final long DEFAULT_CACHE_REQUEST_TIMEOUT_MS = 10000L;
    public static final int DEFAULT_CACHE_MAX_MESSAGES = 0; // 0 = unlimited per Solace API
    public static final int ERROR_CODE_CACHE_ERROR = 700110;

    // Result envelope keys (poll/browse/cache responseVariable shape)
    public static final String RESULT_MESSAGES = "messages";
    public static final String RESULT_MESSAGE_COUNT = "messageCount";
    public static final String RESULT_RECEIVED = "received";
    public static final String RESULT_DESTINATION = "destination";
    public static final String RESULT_TIMED_OUT = "timedOut";

    // 2.0.0 parameter and property names
    public static final String TARGET = "target";
    public static final String TARGET_INBOUND = "INBOUND";
    public static final String TARGET_POLL = "POLL";
    public static final String ACK_MODE = "ackMode";
    public static final String ACK_MODE_AUTO = "AUTO";
    public static final String ACK_MODE_CLIENT = "CLIENT";
    public static final String POLL_ACK_TIMEOUT = "pollAckTimeout";
    public static final String POLL_FLOW_IDLE_TIMEOUT = "pollFlowIdleTimeout";
    public static final String MAX_POLL_FLOWS = "maxPollFlows";
    public static final String REQUIRE_SETTLEMENT_OUTCOMES = "requireSettlementOutcomes";
    public static final String MAX_CONCURRENT_REQUESTS = "maxConcurrentRequests";
    public static final String RECOVERY_INITIAL_DELAY = "recovery.initialDelay";
    public static final String RECOVERY_PROGRESSION = "recovery.progression";
    public static final String RECOVERY_MAX_DELAY = "recovery.maxDelay";
    public static final String RECOVERY_MAX_ATTEMPTS = "recovery.maxAttempts";
    public static final String CLIENT_NAME_STRICT = "clientNameStrict";
    public static final String OAUTH2_TOKEN_FILE = "oauth2TokenFile";
    public static final String KEEP_ALIVE_INTERVAL = "keepAliveInterval";
    public static final String KEEP_ALIVE_LIMIT = "keepAliveLimit";
    public static final String SOLACE_INBOUND_SETTLEMENT = "solace.inbound.settlement";
    public static final String SOLACE_INBOUND_TX = "solace.inbound.tx";
    public static final String SOLACE_INBOUND_TX_PRODUCER = "solace.inbound.tx.producer";
    public static final String SOLACE_POLL_MESSAGE = "solace.poll.message";
    public static final String MODE_DEFERRED = "DEFERRED";
    public static final String MODE_DIRECT = "DIRECT";
    public static final String REASON_ALREADY_DECIDED = "ALREADY_DECIDED";
    public static final String REASON_LATE = "LATE";
    public static final String REASON_STALE_DELIVERY = "STALE_DELIVERY";
    public static final String OUTCOME_UNKNOWN = "UNKNOWN";
    public static final String REQUIRE_OUTCOMES_AUTO = "AUTO";
    public static final String REQUIRE_OUTCOMES_TRUE = "TRUE";
    public static final long DEFAULT_POLL_ACK_TIMEOUT_MS = 180000L;
    public static final long DEFAULT_POLL_FLOW_IDLE_TIMEOUT_MS = 60000L;
    public static final int DEFAULT_MAX_POLL_FLOWS = 16;
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 100;
    public static final long POLL_SLICE_MS = 1000L;
    // Bound of a settle or expiry call into a flow owner: one slice plus the settle round trip.
    public static final long OWNER_CALL_TIMEOUT_MS = 7000L;
    // Bound of a commit or rollback command wait.
    public static final long TX_COMPLETION_WAIT_MS = 30000L;
    // Applied when the init template gives no maxWaitTime.
    public static final String DEFAULT_POOL_MAX_WAIT_MS = "10000";

    // 2.0.0 fault names, the first token of every fault message
    public static final String FAULT_CONNECTION_ERROR = "CONNECTION_ERROR";
    public static final String FAULT_PUBLISH_ERROR = "PUBLISH_ERROR";
    public static final String FAULT_INVALID_CONFIG = "INVALID_CONFIG";
    public static final String FAULT_REQUEST_REPLY_ERROR = "REQUEST_REPLY_ERROR";
    public static final String FAULT_ACK_ERROR = "ACK_ERROR";
    public static final String FAULT_NACK_ERROR = "NACK_ERROR";
    public static final String FAULT_POLL_ERROR = "POLL_ERROR";
    public static final String FAULT_TRANSACTION_ACTIVE = "TRANSACTION_ACTIVE";
    public static final String FAULT_TRANSACTION_COMPLETED = "TRANSACTION_COMPLETED";
    public static final String FAULT_INBOUND_TRANSACTION = "INBOUND_TRANSACTION";
    public static final String FAULT_REQUEST_IN_TRANSACTION = "REQUEST_IN_TRANSACTION";
    public static final String FAULT_POLL_UNSETTLED = "POLL_UNSETTLED";
    public static final String FAULT_POLL_FLOW_LIMIT = "POLL_FLOW_LIMIT";
    public static final String FAULT_REQUEST_CONCURRENCY_EXCEEDED = "REQUEST_CONCURRENCY_EXCEEDED";
    public static final String FAULT_DEADLINE_BEFORE_SEND = "DEADLINE_BEFORE_SEND";
    public static final String FAULT_DEADLINE = "DEADLINE";
    public static final String FAULT_EXPIRED = "EXPIRED";
    public static final String FAULT_UNKNOWN = "UNKNOWN";
    public static final String FAULT_NACK = "NACK";
    public static final String FAULT_ROLLED_BACK = "ROLLED_BACK";

    // 2.0.0 error codes
    public static final int ERROR_CODE_TRANSACTION_ACTIVE = 700111;
    public static final int ERROR_CODE_TRANSACTION_COMPLETED = 700112;
    public static final int ERROR_CODE_INBOUND_TRANSACTION = 700113;
    public static final int ERROR_CODE_REQUEST_IN_TRANSACTION = 700114;
    public static final int ERROR_CODE_POLL_UNSETTLED = 700115;
    public static final int ERROR_CODE_POLL_FLOW_LIMIT = 700116;
    public static final int ERROR_CODE_REQUEST_CONCURRENCY_EXCEEDED = 700117;
    public static final int ERROR_CODE_DEADLINE_BEFORE_SEND = 700118;
    public static final int ERROR_CODE_DEADLINE = 700119;
    public static final int ERROR_CODE_EXPIRED = 700120;
    public static final int ERROR_CODE_UNKNOWN = 700121;
    public static final int ERROR_CODE_NACK = 700122;
    public static final int ERROR_CODE_ROLLED_BACK = 700123;
}
