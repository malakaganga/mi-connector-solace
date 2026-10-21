# mi-connector-solace
The Solace connector enables WSO2 Integrator: MI to publish messages and interact with a Solace broker. It supports reliable, asynchronous communication, allowing you to send messages to queues or topics, and manage message acknowledgments.

## Operations

All operations are invoked with the `solace.` prefix and act on a configured connection. Configure the connection once (host, VPN, and credentials) and reference it from each operation.

### Messaging

| Operation | Description |
|---|---|
| `solace.publishMessage` | Publish a message to a topic or queue. Supports `DIRECT` (topics only), `PERSISTENT` and `NON_PERSISTENT` delivery. On guaranteed delivery the operation waits for the broker acknowledgement by default (`waitForAck` true) under the `ackTimeout` deadline (10000 ms). When the deadline cuts the operation off after the send was entered, the fault carries `outcome=UNKNOWN`: the message may or may not have reached the broker, so a retry is at least once. |
| `solace.sendRequest` | Send a request message and block until a reply arrives or `requestTimeout` expires. A guaranteed request is published on the connection's request reply session with `waitForAck` true and `ackTimeout` 10000 ms by default, and the same `outcome=UNKNOWN` rule applies. |
| `solace.sendReply` | Send a reply to a request message, using the reply to destination of the message the inbound endpoint stashed (`target=INBOUND`, the default) or of the message a poll with `ackMode=CLIENT` stashed (`target=POLL`). `waitForAck` and `ackTimeout` apply as for `publishMessage`, so a reply cut off by the deadline after the send was entered faults with `outcome=UNKNOWN`. Pairs with `solace.sendRequest`. |

### Consuming

| Operation | Description |
|---|---|
| `solace.poll` | Poll one message from a queue on a cached flow owned by one thread per queue and selector. The result is a timed out envelope when the queue is empty. `ackMode=AUTO` (default) acknowledges the message right after it is handed to the call. `ackMode=CLIENT` stashes the message under `solace.poll.message` with a lease of `pollAckTimeout`, for `acknowledgeMessage` or `nackMessage` with `target=POLL`. Inside a connector owned transaction the message is held until commit or rollback. |
| `solace.browse` | Browse messages on a queue without consuming them — useful for diagnostics and dead-message-queue inspection. |
| `solace.acknowledgeMessage` | Settle a message ACK. With `target=INBOUND` (default) the decision is handed to inbound endpoint 2.0.0 through its settlement handle (`mode=DEFERRED`) or settled directly with inbound endpoint 1.x (`mode=DIRECT`). With `target=POLL` the lease of the polled message is settled on its flow owner. A second call reports `settled=false` with the reason `ALREADY_DECIDED`, `LATE` or `STALE_DELIVERY`. |
| `solace.nackMessage` | Settle a message `FAILED` (the broker redelivers it) or `REJECTED` (the broker moves it to the dead message queue), with the same `target` choice and the same reporting. |

### Transactions

| Operation | Description |
|---|---|
| `solace.beginTransaction` | Begin a connector owned transaction. It pins one pooled session and gives the transaction its own thread. Every `publishMessage`, `sendReply` and `poll` carrying the transaction runs on that thread. |
| `solace.commit` | Commit the active transaction. |
| `solace.rollback` | Roll back the active transaction (polled messages are redelivered). |

## Connection

Each connection name holds one logical connection. It has a pool of sessions for `publishMessage`, `browse` and transactions, plus two dedicated sessions created on first use, one for the `poll` flows and one for request reply. The broker's per client username connection limit must therefore allow the pool size plus two.

Client names are deterministic. A pooled session is named `<clientName>-<node>-<connectionName>-<n>` with a counter per logical connection, and the dedicated sessions end in `-rr` and `-poll`. With `clientNameStrict=true` the first pooled session uses `clientName` as given and the other sessions append their role. A rebuilt session reuses the name of the session it replaces, so the broker evicts the old one. The node is the node id of WSO2 Integrator: MI, read from the `node.id` system property, else the `nodeId` system property or environment variable, else `[cluster_config] node_id` in deployment.toml, else the host name. Two instances on one host need distinct node ids.

A session that failed is rebuilt on its next use with the `recovery.initialDelay`, `recovery.progression`, `recovery.maxDelay` and `recovery.maxAttempts` parameters, within the operation's deadline. A session rebuilt while the broker is unreachable holds the operation for `ackTimeout` at most. The connect attempt itself ends on its own thread at the Solace connect timeout. The pool borrow is bounded by `maxWaitTime` (default 10000 ms) and not by the operation's `ackTimeout`. `ackTimeout` covers the borrow only when `maxWaitTime` is at most `ackTimeout`.

Each physical connect attempt connects once inside the Solace client. On a pooled session one call makes its first attempt and then up to `connectRetries` more, with the recovery back off between them and always within the operation's deadline. The default of 0 means one attempt per call, and -1 means no limit other than the deadline and `recovery.maxAttempts`. The request reply and poll sessions make one attempt per creation, and the next call starts the next one. A session has at most one attempt in progress, and a caller that arrives meanwhile waits for it under its own deadline. A session that failed, or was replaced, keeps its slot until its close has finished, so an outage never piles up connect attempts or closes behind callers that gave up.

A call that arrives while the dedicated session it needs is being created waits for that creation within its own deadline. It then uses the session, or faults with the creation's failure, or faults with `EXPIRED` when its deadline passes first.

Poll flows are cached per queue and selector, at most `maxPollFlows` per connection (default 16), each with its own owner thread. An idle flow with no outstanding lease is closed after `pollFlowIdleTimeout` (default 60000 ms). When the limit is reached and no flow is idle, `poll` faults with `POLL_FLOW_LIMIT`. A message polled with `ackMode=CLIENT` holds a lease of `pollAckTimeout` (default 180000 ms). When the lease passes unsettled, the connector settles the message `FAILED` and the broker redelivers it. When the broker rebinds a poll flow, the connector closes that flow and the next poll binds a fresh one. A message that was buffered before the rebind is never returned by a poll, the broker redelivers it. A message whose lease ended comes back from the broker in the same way. The poll path is at least once, a redelivered message can reach a sequence that already processed it.

`requireSettlementOutcomes` controls the negotiation of the `FAILED` and `REJECTED` outcomes on the poll flows. `AUTO` (default) requests the outcomes the broker reports and, for a missing one, retains the message and recycles the flow so the broker redelivers it. `TRUE` fails the flow bind when the broker lacks one.

`maxConcurrentRequests` (default 100) bounds the `sendRequest` calls in flight on the request reply session. A call beyond it waits up to its `requestTimeout` and then faults with `REQUEST_CONCURRENCY_EXCEEDED`. Every session keeps the Solace publisher acknowledgement window of 1 unless the connection sets `pubAckWindowSize`. At window 1 the sends of the request reply session are serialised on the broker acknowledgement, about one round trip per request. A larger window helps only when the request stream keeps the window busy. A sparse stream pays the broker's acknowledgement timer of about one second per confirmation instead. At window 1 the producer lock is held through the confirmation wait, so the callers queue on the lock and never wait inside the Solace client for publisher window space. The other calls into the Solace client can still block, each under the operation's deadline. Two cases follow for a request whose deadline passes. An expiry while the caller waits to enter the send leaves the shared session alive, and the caller faults `DEADLINE_BEFORE_SEND` with nothing sent. An expiry after the send was admitted can retire the shared session once, the results of the callers in flight at that instant are `UNKNOWN`, and the next caller rebuilds the session. A retry after an `UNKNOWN` result is at least once.

## Transactions

A connector owned transaction starts with `beginTransaction`, which pins one pooled session and gives the transaction its own thread. Every `publishMessage`, `sendReply` and `poll` inside it is a command on that thread, and the caller waits at most its `ackTimeout` (`pollTimeout` for a poll). `commit` and `rollback` wait at most 30000 ms for the completion, which always runs to its end on the transaction's thread. A transaction that is not completed within `transactionTimeoutMillis` is rolled back by the connector.

An inbound owned transaction is the transacted mode of inbound endpoint 2.0.0 (`solace.transacted=true`). The inbound endpoint commits or rolls back. `publishMessage` and `sendReply` publish on its transacted producer, and `acknowledgeMessage` and `nackMessage` decide the outcome as in every other mode.

| Fault | Code | When |
|---|---|---|
| `INBOUND_TRANSACTION` | 700113 | `beginTransaction`, `commit`, `rollback` or `poll` inside an inbound owned transaction. Acknowledge or nack the message instead. |
| `REQUEST_IN_TRANSACTION` | 700114 | `sendRequest` inside any transaction. A transacted publish is invisible to the responder until the commit, so the reply could never arrive before it. |
| `TRANSACTION_COMPLETED` | 700112 | An operation submitted to a transaction that has ended, or still queued when it ended. |
| `ROLLED_BACK` | 700123 | `commit` found that the broker rolled the transaction back. Nothing was committed and the session stays usable. |
| `UNKNOWN` | 700121 | The outcome is unknown. An operation cut off while its command was running makes the transaction uncertain, so the next `commit` is refused and rolls back. A commit whose result the broker did not confirm, or a rollback that failed, faults `UNKNOWN` and the pinned session is discarded. |

## Compatibility

Connector 2.0.0 works with inbound endpoint 1.x. The operations find no settlement handle on the message context and settle the message directly, which the response reports as `mode=DIRECT`. Inbound endpoint 2.0.0 requires connector 2.0.0 and refuses to start while a 1.x connector is deployed.

## Prerequisites

The Solace JCSMP client (`sol-jcsmp`) is **not bundled** with this connector. Solace JCSMP is not Apache-2.0 licensed, so it cannot be redistributed or repackaged. You must install it once into the MI server's shared library directory, `<MI_HOME>/lib`, before deploying.

Installing it in `<MI_HOME>/lib` (rather than bundling it inside the connector) is required so that this connector and the [Solace inbound endpoint](https://github.com/wso2-extensions/mi-inbound-solace) share a **single** copy of the client. A single shared copy:

- avoids a Netty initialization clash — `java.lang.IllegalArgumentException: 'TOTAL_SOCKET_BYTES_SENT' is already in use` — that occurs when two copies of the JCSMP Netty transport are loaded in the same server (e.g. the connector and the inbound deployed together, or a hot-redeploy), and
- lets the inbound endpoint hand its received message to this connector's `acknowledge` / `nack` operations (both modules must resolve `com.solacesystems.jcsmp.BytesXMLMessage` from the same classloader).

### Install the required JARs

Place the following into `<MI_HOME>/lib` and restart the server:

| Artifact | Version |
|---|---|
| `com.solacesystems:sol-jcsmp` | `10.30.1` |
| `org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib` | `1.1.3_2` |

Netty does **not** need to be added — the JCSMP client uses the Netty already shipped with MI (MI 4.6.0 bundles `netty-all 4.2.12`, which matches `sol-jcsmp 10.30.x`).

You can fetch both JARs from Maven into `lib` with:

```bash
mvn dependency:copy -Dartifact=com.solacesystems:sol-jcsmp:10.30.1 -DoutputDirectory="$MI_HOME/lib"
mvn dependency:copy -Dartifact=org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib:1.1.3_2 -DoutputDirectory="$MI_HOME/lib"
```

Then restart WSO2 MI.

## Example

Once the connection is configured (host, VPN, and credentials) and referenced by a key — e.g. `MY_SOLACE` — operations reference it via `configKey`:

```xml
<!-- Publish a message to a topic -->
<solace.publishMessage configKey="MY_SOLACE">
    <destinationType>TOPIC</destinationType>
    <destinationName>orders/new</destinationName>
    <messageType>TEXT</messageType>
    <responseVariable>solacePublishResponse</responseVariable>
</solace.publishMessage>

<!-- Synchronously poll one message from a queue -->
<solace.poll configKey="MY_SOLACE">
    <queueName>orders.q</queueName>
    <pollTimeout>5000</pollTimeout>
    <responseVariable>solacePollResponse</responseVariable>
</solace.poll>
```
