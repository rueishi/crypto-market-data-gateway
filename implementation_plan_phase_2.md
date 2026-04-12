# Implementation Plan - Phase 2 Task Cards

This file decomposes **Phase 2 - Coinbase L2 Connector (live feed, full recovery deferred)** from `crypto_market_data_gateway_spec_v2.md` into execution-ready task cards for AI and human developers.

Phase 2 scope from the spec:
- Build the production `COINBASE_L2` venue package and the shared runtime pieces explicitly required by Phase 2
- Implement real Netty WebSocket transport behavior
- Implement Coinbase auth/subscribe, parser, connector, sequencing, snapshot-boundary handling, liveness detection, and fixture/integration tests
- Implement only the metric counters and gauges explicitly assigned to Phase 2
- Keep full recovery, `/metrics`, latency counters, graceful shutdown, and second-venue work deferred

Primary spec references:
- Sections 3, 4, 5, 6, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 20, 24, 25, 26
- Phase 2 ACs: AC-1, AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-9, AC-10, AC-12, AC-15, AC-16, AC-17, AC-18, AC-41, AC-44, AC-45, AC-48, AC-52, AC-53, AC-54, AC-55, AC-56, AC-57, AC-58, AC-59, AC-60, AC-62

Recommended implementation order:
1. `P2-000` Shared Phase 2 Support and Venue Config
2. `P2-001` Netty WebSocket Transport
3. `P2-002` Coinbase Authentication and Subscription Builder
4. `P2-003` Coinbase L2 Feed Parser
5. `P2-004` Coinbase Snapshot Strategy and L2 Recovery Stub
6. `P2-005` Coinbase L2 Connector and Factory Wiring
7. `P2-006` Phase 2 Counters and Liveness Monitoring
8. `P2-007` Phase 2 Fixture, Integration, and Structural Verification

Standard execution rule for every card:
- Read the card fully.
- Read all `Required Existing Inputs`.
- Treat `Required Spec Extracts` as the minimum authoritative spec slice for the task.
- Modify only files listed in `Expected modified files` and `Test classes/files` unless a dependency forces a small adjacent change.
- If you must touch a file outside the listed set, record it in the task completion notes.
- Treat `Out of Scope` as hard boundaries for the task.

---

## Task Card P2-000

### 1. Task ID
`P2-000`

### 2. Task Name
Shared Phase 2 Support and Venue Config

### 3. Task Description
Implement the shared support Phase 2 needs before the real Coinbase L2 connector can be wired: typed Coinbase venue config, Coinbase authenticator support, and the minimal extensions to shared counters used by the live connector path.

### 4. Spec References
- Sections 6.6, 9.1-9.3, 14, 20, 24, 26

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 6.6, 9.1, 9.2, 9.3, 14, 20, 24, 25, 26
- `implementation_plan_phase_1.md`
- Phase 1 outputs for config, counters, publisher, and contexts

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/CoinbaseConfig.java`
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/CoinbaseAuthenticator.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/observability/GatewayCounters.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/observability/InstrumentCounters.java`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/core/config/TransportConfig.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/...`
- `src/test/java/io/rueishi/marketdata/crypto/core/observability/...`

Do not modify unless necessary:
- Phase 3 observability endpoint files

### 4b. Required Spec Extracts
- Venue config is parsed by the connector factory from the raw venue config map into a typed config object.
- Coinbase auth/signature data is needed by authenticated subscribe/unsubscribe payload construction.
- `GatewayCounters` allocates `InstrumentCounters`.
- Phase 2 observability is limited to live-path counters and gauges.

### 4c. Implementation Guidelines For This Task
- Keep Coinbase config immutable and explicit.
- Keep authenticator responsibilities narrow: timestamp, signature, auth fields.
- Add only the counters explicitly needed by Phase 2.

### 4d. Expanded Authoritative Spec Excerpts
- Coinbase uses authenticated WebSocket subscribe/unsubscribe payloads for `level2` and `heartbeat`.
- Phase 2 requires counters for heartbeats, unknown types, validation failures, and active connections.
- Shared counters and publisher are created before connector construction.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-4, AC-5, AC-6, AC-7, AC-9, AC-41, AC-44, AC-45, AC-52 support ownership

Tests to add:
- `CoinbaseConfigTest`
- `CoinbaseAuthenticatorTest`
- `Phase2InstrumentCountersTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: valid venue config map and instrument config
- Output: typed Coinbase config, usable authenticator, and live-path counters
- Logic: later cards need stable shared support before transport/parser/connector work can be wired

Edge path:
- Input: optional config fields absent but not required for a given test
- Output: typed config still builds with explicit defaults
- Logic: keep test-time configuration predictable

Negative path:
- Input: malformed venue config or empty credential source
- Output: parse/validation failure before connector creation
- Logic: fail closed before network logic begins

Exception path:
- Input: unsupported signature precondition or malformed secret data
- Output: explicit exception with no secret leakage
- Logic: auth failures must be diagnosable without exposing credentials

Failure path:
- Input: shared counter APIs drift later across cards
- Output: parser/connector/tests diverge
- Logic: centralize shared support ownership here first

### 7. Implementation Steps
Classes/files:
- `venue/coinbase/shared/CoinbaseConfig.java`
- `venue/coinbase/shared/CoinbaseAuthenticator.java`
- `core/observability/GatewayCounters.java`
- `core/observability/InstrumentCounters.java`

Step-by-step:
1. Define immutable `CoinbaseConfig` from the raw venue config block.
2. Implement `CoinbaseAuthenticator` for Coinbase timestamp/signature generation.
3. Extend `InstrumentCounters` with only the Phase 2-required counters and gauges.
4. Extend `GatewayCounters` allocation methods so each instrument gets the new counter set.
5. Document which observability work remains deferred to Phase 3.

Class stub:
```java
public record CoinbaseConfig(
        URI endpoint,
        String apiKey,
        String apiSecret,
        String apiPassphrase,
        List<String> channels) {
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/CoinbaseConfigTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/CoinbaseAuthenticatorTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/observability/Phase2InstrumentCountersTest.java`

Step-by-step:
1. Verify valid Coinbase config parses correctly and invalid config fails cleanly.
2. Verify signature construction is deterministic for a fixed timestamp and payload.
3. Verify secrets never appear in exception messages or debug output.
4. Verify required Phase 2 counter and gauge accessors exist.

Test class stub:
```java
class CoinbaseAuthenticatorTest {
    @Test
    void signsSubscribePayloadDeterministically() { }
}
```

### 9. Out of Scope
- Metrics REST endpoint
- DistinctErrorLog
- Full recovery logic
- Graceful shutdown

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-001

### 1. Task ID
`P2-001`

### 2. Task Name
Netty WebSocket Transport

### 3. Task Description
Implement the real shared `NettyWebSocketTransport` for Phase 2, including TLS, WebSocket handshake, frame routing to the parser callback surface, frame-size enforcement, close behavior, connection event callbacks, event-loop access, and injectable test hooks. Counter/gauge mutation and liveness scheduling are owned by connector/observability cards, not by the transport.

### 4. Spec References
- Sections 3.1, 4.6, 5.3, 5.8, 14, 15, 18, 26
- AC-1, AC-10, AC-48, AC-52

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 3.1, 4.6, 5.3, 5.8, 14, 15, 18, 25, 26
- outputs from P2-000
- Phase 1 transport interfaces/placeholders

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/core/transport/NettyWebSocketTransport.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/transport/WebSocketFrameHandler.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/transport/PipelineCustomizer.java`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/core/connector/AbstractConnector.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/transport/...`

Do not modify unless necessary:
- Coinbase parser logic
- Phase 3 recovery behavior

### 4b. Required Spec Extracts
- `NettyWebSocketTransport` is a concrete shared transport in `core/transport`.
- One connector owns one transport.
- Reconnect responsibility is not inside the transport.
- Counter/gauge mutation is not inside the transport; the transport exposes connection events so the connector can update observability state.
- On Linux, the gateway prefers epoll and BoringSSL when Netty reports the
  native providers are available, with portable NIO/JDK fallback for non-Linux
  development and test environments.
- Time-dependent behavior must remain testable via injectable clocks.

### 4c. Implementation Guidelines For This Task
- Keep reconnect orchestration out of transport.
- Keep event-loop ownership explicit.
- Route inbound text/binary/close/disconnect events into the connector-facing contract with minimal allocation.

### 4d. Expanded Authoritative Spec Excerpts
- Shared transport handles TLS handshake, WebSocket upgrade, frame-size enforcement, and frame dispatch.
- Heartbeat timeout detection is Phase 2 detection only, not recovery action, and is scheduled by connector/liveness code after subscription ack validation.
- Transport close closes the channel only; higher-level reconnect flow belongs outside transport.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-1, AC-10, AC-48, AC-52

Tests to add:
- `NettyWebSocketTransportTest`
- `NettyWebSocketTransportLinuxConfigTest`
- `NettyWebSocketTransportClockInjectionTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: valid endpoint, pipeline setup, and frame callbacks
- Output: connected/auth-ready WebSocket transport that delivers frames to connector/parser code
- Logic: transport is the bridge between Netty and venue logic

Edge path:
- Input: empty frame, ping/pong, or benign close frame
- Output: handled without parser corruption or stuck connection state
- Logic: keep control-plane handling explicit

Negative path:
- Input: oversized frame or handshake failure
- Output: channel close/failure callback without partial parser execution
- Logic: fail before malformed data reaches venue parsing

Exception path:
- Input: callback throws on event-loop thread
- Output: close channel and propagate failure through connector-owned error path
- Logic: avoid undefined transport state after callback exceptions

Failure path:
- Input: transport tries to reconnect automatically
- Output: phase-boundary violation
- Logic: transport must stay single-responsibility

### 7. Implementation Steps
Classes/files:
- `core/transport/NettyWebSocketTransport.java`
- `core/transport/WebSocketFrameHandler.java`
- `core/transport/PipelineCustomizer.java`

Step-by-step:
1. Replace Phase 1 placeholders with real transport contracts needed by `AbstractConnector`.
2. Implement `NettyWebSocketTransport` constructor dependencies explicitly.
3. Implement connect/open/close lifecycle and WebSocket handshake.
4. Implement inbound frame dispatch to text/binary callbacks.
5. Implement Linux epoll/BoringSSL selection hooks per spec: bootstrap supplies
   an `EpollEventLoopGroup` when `Epoll.isAvailable()` is true, falls back to
   `NioEventLoopGroup` otherwise, and transport selects the compatible channel
   class from the supplied event-loop group.
6. Expose event-loop and connection-state hooks needed by connector-owned liveness and gauge updates, without scheduling liveness checks or mutating counters in the transport.

Class stub:
```java
public final class NettyWebSocketTransport implements AutoCloseable {
    public void connect() { }
    public void sendText(byte[] utf8Payload) { }
    @Override public void close() { }
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/core/transport/NettyWebSocketTransportTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/transport/NettyWebSocketTransportLinuxConfigTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/transport/NettyWebSocketTransportClockInjectionTest.java`

Step-by-step:
1. Verify connect/send/close wiring through a mock or embedded WebSocket server.
2. Verify Linux transport configuration picks epoll/BoringSSL when appropriate and the portable NIO/JDK fallback is covered when native providers are unavailable.
3. Verify event-loop/clock test hooks are available for connector-owned liveness tests without wall clock sleeps.

Test class stub:
```java
class NettyWebSocketTransportTest {
    @Test
    void deliversInboundTextFramesToHandler() { }
}
```

### 9. Out of Scope
- Reconnect orchestration
- Graceful shutdown deadline behavior
- Metrics REST endpoint

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-002

### 1. Task ID
`P2-002`

### 2. Task Name
Coinbase Authentication and Subscription Builder

### 3. Task Description
Implement Coinbase L2 subscribe/unsubscribe payload construction, including authenticated HMAC fields and the exact `level2` and `heartbeat` channel requirements.

### 4. Spec References
- Sections 5.4, 14, 24, 25, 26
- AC-9, AC-10

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 5.4, 14, 24, 25, 26
- outputs from P2-000

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SubscriptionBuilder.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/subscription/SubscriptionBuilder.java`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/CoinbaseAuthenticator.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SubscriptionBuilderTest.java`

Do not modify unless necessary:
- parser logic
- connector lifecycle logic

### 4b. Required Spec Extracts
- Authenticated subscribe is sent after connection establishment.
- Required channels are `level2` and `heartbeat`.
- Phase 2 uses one authenticated subscribe frame whose `channels` array contains both `level2` and `heartbeat`; unsubscribe mirrors that same single-frame control-plane scope with `"type": "unsubscribe"`.
- Ack validation tolerates extra channels but requires the expected channels and correct `product_id`.
- Unsubscribe exists as a contract even though full recovery/graceful-shutdown behavior is deferred.

### 4c. Implementation Guidelines For This Task
- Build UTF-8 JSON bytes directly.
- Keep payload field ordering stable for deterministic tests.
- Make builder methods explicit for subscribe and unsubscribe.

### 4d. Expanded Authoritative Spec Excerpts
- Coinbase outbound subscribe and unsubscribe payloads include auth fields generated by the venue authenticator.
- The single subscribe/unsubscribe payload shape is an explicit Phase 2 plan choice so `SubscriptionBuilder.buildSubscribe(...)` and `buildUnsubscribe(...)` can continue returning one UTF-8 JSON byte array.
- `SubscriptionBuilder.buildSubscribe(...)` and `buildUnsubscribe(...)` return UTF-8 JSON bytes.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-9 support, AC-10

Tests to add:
- `CoinbaseL2SubscriptionBuilderTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: valid instrument and Coinbase auth config
- Output: one authenticated subscribe payload containing `channels: ["level2", "heartbeat"]`
- Logic: connector must send exact control-plane payloads expected by Coinbase

Edge path:
- Input: extra optional channels configured for tests
- Output: required channels still present in the single payload and stable
- Logic: ack validation is tolerant of extras but not missing required channels

Negative path:
- Input: missing auth material
- Output: builder/auth failure before send
- Logic: do not send malformed payloads

Exception path:
- Input: JSON building/signature generation error
- Output: explicit exception before network send
- Logic: control-plane failures should fail fast

Failure path:
- Input: subscribe payload omits `heartbeat`
- Output: liveness and ack validation fail later
- Logic: builder correctness is the earliest protection point

### 7. Implementation Steps
Classes/files:
- `venue/coinbase/l2/CoinbaseL2SubscriptionBuilder.java`
- `core/subscription/SubscriptionBuilder.java`

Step-by-step:
1. Confirm the shared `SubscriptionBuilder` contract is sufficient for Phase 2.
2. Implement one authenticated subscribe payload containing both `level2` and `heartbeat`.
3. Implement one authenticated unsubscribe payload for the same instrument/channels.
4. Add deterministic tests for payload body and required fields.

Class stub:
```java
public final class CoinbaseL2SubscriptionBuilder implements SubscriptionBuilder {
    @Override public byte[] buildSubscribe(InstrumentConfig instrument) { return new byte[0]; }
    @Override public byte[] buildUnsubscribe(InstrumentConfig instrument) { return new byte[0]; }
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SubscriptionBuilderTest.java`

Step-by-step:
1. Verify the single subscribe payload includes auth fields, product id, and both required channels.
2. Verify unsubscribe payload mirrors the same control-plane scope.
3. Verify deterministic output for fixed clock/signature inputs.

### 9. Out of Scope
- Triggering recovery from invalid ack
- Transport send timing

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-003

### 1. Task ID
`P2-003`

### 2. Task Name
Coinbase L2 Feed Parser

### 3. Task Description
Implement `CoinbaseL2FeedParser` for the supported Coinbase Exchange Direct Feed messages in Phase 2: `snapshot`, `l2update`, `heartbeat`, and `subscriptions` acknowledgement. This card owns the real parser behavior, including unknown-type handling, symbol/product validation, counters updates, timestamp extraction, and normalized encode/publish calls.

### 4. Spec References
- Sections 4.6, 5.3, 14, 16, 17, 24, 25, 26
- AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-9, AC-12, AC-15, AC-16, AC-17, AC-18, AC-53, AC-54, AC-55, AC-56, AC-57, AC-58, AC-59, AC-60

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 4.6, 5.3, 14, 16, 17, 24, 25, 26
- outputs from Phase 1 `ByteBufScanner`, `SbeEncoder`, contexts, and publisher
- outputs from P2-000 and P2-002

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2FeedParser.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/parser/FeedParser.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/parser/ParseContext.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/parser/DefaultParseContext.java`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/core/parser/ByteBufScanner.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/connector/AbstractConnector.java`
- `src/test/resources/fixtures/coinbase/l2/...`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/...`

Do not modify unless necessary:
- full recovery logic
- transport implementation beyond callback contracts

### 4b. Required Spec Extracts
- Supported inbound message types are `snapshot`, `l2update`, `heartbeat`, and `subscriptions`.
- `heartbeat` updates health state and counters only.
- `subscriptions` ack validates required channels and product id; extra channels are tolerated.
- Successful `subscriptions` ack validation must signal connector/session state through the parse context so connector-owned liveness can start after the ack, not at transport connect time.
- Bad `subscriptions` ack or product mismatch triggers a Phase 2 control-plane validation failure event: count, log, and drop only; do not call `recover()` in Phase 2.
- Pre-snapshot updates are dropped until first snapshot acceptance.
- Coinbase snapshot exchange timestamp uses the top-level `time` field if present and non-zero, otherwise encode `-1`.

### 4c. Implementation Guidelines For This Task
- Keep parser stateless across frames except through passed contexts.
- Reuse `ByteBufScanner` utilities instead of building ad hoc helpers.
- Make snapshot/update/heartbeat/control-plane branches explicit and easy to test.

### 4d. Expanded Authoritative Spec Excerpts
- `snapshot` encodes `BOOK_SNAPSHOT`.
- `l2update` encodes `BOOK_UPDATE`, with size `> 0` as `UPSERT` and size `0` as `DELETE`.
- Unknown or unsupported message types are ignored and counted.
- `product_id` mismatch is rejected, counted, and not published.
- Control-plane validation failures are reported through context/counters/logging only in Phase 2. They must not invoke recovery or reconnect until Phase 3 owns that behavior.
- `gatewayMessageSeq`, `seq1`, and `seq2` are all equal to the connector-local sequence tracker for Coinbase L2.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-9, AC-12, AC-15, AC-16, AC-17, AC-18, AC-53, AC-54, AC-55, AC-56, AC-57, AC-58, AC-59, AC-60

Tests to add:
- `CoinbaseL2FeedParserTest`
- `CoinbaseL2SubscriptionsAckTest`
- `CoinbaseL2HeartbeatTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: valid Coinbase `snapshot` or `l2update` frame for the configured product
- Output: correctly encoded and published `BOOK_SNAPSHOT` or `BOOK_UPDATE`
- Logic: parser is the venue-specific normalization point from exchange JSON to shared wire format

Edge path:
- Input: snapshot with missing/epoch-zero `time`, or ack with extra channels
- Output: sentinel `-1` timestamp or successful validation
- Logic: follow exact Coinbase-specific wire/tolerance rules

Negative path:
- Input: unsupported type, product mismatch, bad subscription ack, unknown symbol, or pre-snapshot update
- Output: counted and dropped with no publish; bad ack/product mismatch also emits a control-plane validation failure event without calling `recover()`
- Logic: keep stream state coherent while tolerating irrelevant inbound data

Exception path:
- Input: malformed JSON/frame content
- Output: controlled parser failure path with counter/log updates as applicable
- Logic: malformed frames must not silently corrupt the pipeline

Failure path:
- Input: parser stores session state as fields or bypasses context/session invariants
- Output: recovery/snapshot/session bugs later in Phase 2 and 3
- Logic: parser must remain context-driven and stateless across sessions

### 7. Implementation Steps
Classes/files:
- `venue/coinbase/l2/CoinbaseL2FeedParser.java`
- `core/parser/FeedParser.java`
- `core/parser/ParseContext.java`
- `core/parser/DefaultParseContext.java`

Step-by-step:
1. Confirm or adjust the `FeedParser` text-frame contract for production Phase 2 use.
2. Implement routing by top-level Coinbase `type`.
3. Implement snapshot parsing and encoding path.
4. Implement `l2update` parsing/action mapping path.
5. Implement heartbeat handling that updates counters and last-seen timing without publishing.
6. Add a parse-context signal for successful subscription ack validation, such as `onSubscriptionAckValidated()`, so connector-owned liveness can start only after the ack.
7. Implement `subscriptions` ack validation and counter/log updates.
8. Implement Phase 2 control-plane validation failure behavior for bad ack/product mismatch: count, log, drop, and explicitly do not call `recover()`.
9. Add fixture-driven tests for supported and unsupported paths.

Class stub:
```java
public final class CoinbaseL2FeedParser implements FeedParser {
    @Override
    public void onTextFrame(ByteBuf frame, ParseContext ctx) { }
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2FeedParserTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SubscriptionsAckTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2HeartbeatTest.java`

Step-by-step:
1. Add fixture-driven snapshot/update tests with byte-level decoder assertions.
2. Add ack validation tests for missing channel, wrong product, extra-channel tolerance, successful-ack context signaling, and no recovery invocation on validation failure.
3. Add heartbeat tests verifying counters update and no SBE message is published.
4. Add unknown-type/product-mismatch/pre-snapshot-drop tests.

Test class stub:
```java
class CoinbaseL2FeedParserTest {
    @Test
    void parsesSnapshotAndPublishesBookSnapshot() { }
}
```

### 9. Out of Scope
- Autonomous recovery initiation
- L3 parsing
- Phase 3 allocation-hardening work

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-004

### 1. Task ID
`P2-004`

### 2. Task Name
Coinbase Snapshot Strategy and Recovery Stub

### 3. Task Description
Implement the Coinbase L2 snapshot strategy by extending the shared subscribe-driven strategy and implement the Phase 2 Coinbase L2 recovery stub that immediately reports channel restoration without full reconnect/resubscribe logic.

### 4. Spec References
- Sections 5.5, 5.6, 8.3, 8.4, 14, 25, 26
- AC-12, AC-15, AC-16

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 5.5, 5.6, 8.3, 8.4, 14, 25, 26
- outputs from Phase 1 snapshot/recovery base classes
- outputs from P2-003

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SnapshotStrategy.java`
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2RecoveryStrategy.java`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/core/snapshot/SubscribeDrivenSnapshotStrategy.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/recovery/ReconnectRecoveryStrategy.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/...`

Do not modify unless necessary:
- Phase 3 recovery semantics

### 4b. Required Spec Extracts
- Coinbase L2 is `SUBSCRIBE_DRIVEN`.
- Gate opens on snapshot and `onSnapshotBoundaryAccepted()` is called.
- Phase 2 recovery strategy is a stub only; full reconnect/resubscribe belongs to Phase 3.

### 4c. Implementation Guidelines For This Task
- Keep snapshot acceptance behavior minimal and exact.
- Do not silently implement full recovery behavior under the stub.
- Make the stub boundary explicit in comments and tests.

### 4d. Expanded Authoritative Spec Excerpts
- Snapshot strategy accepts the first valid snapshot boundary after subscribe/connect and opens the gate.
- Phase 2 tracks coherent stream state but defers autonomous recovery to Phase 3.
- Coinbase recovery stub may call `onChannelRestored()` immediately to satisfy wiring without owning full recovery logic.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-12, AC-15, AC-16

Tests to add:
- `CoinbaseL2SnapshotStrategyTest`
- `CoinbaseL2RecoveryStrategyTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: first valid snapshot after subscribe/connect
- Output: accepted boundary and opened gate
- Logic: snapshot strategy is the session-boundary owner for Coinbase L2

Edge path:
- Input: duplicate or late snapshot after boundary already established
- Output: handled without corrupting gate state
- Logic: strategy must preserve coherent session semantics

Negative path:
- Input: update arrives before snapshot
- Output: gate closed and update dropped
- Logic: no buffering in Phase 2

Exception path:
- Input: invalid snapshot acceptance path
- Output: explicit strategy failure rather than silent state corruption
- Logic: boundary mistakes become severe downstream if hidden

Failure path:
- Input: recovery stub grows real reconnect logic
- Output: Phase 2/3 ownership drift
- Logic: keep stub explicit and minimal

### 7. Implementation Steps
Classes/files:
- `venue/coinbase/l2/CoinbaseL2SnapshotStrategy.java`
- `venue/coinbase/l2/CoinbaseL2RecoveryStrategy.java`

Step-by-step:
1. Extend `SubscribeDrivenSnapshotStrategy` with Coinbase L2-specific class.
2. Verify snapshot acceptance flow calls `onSnapshotBoundaryAccepted()` exactly at the right point.
3. Implement `CoinbaseL2RecoveryStrategy` as a documented stub that reports restored channel immediately.
4. Add tests distinguishing the stub from real Phase 3 recovery behavior.

Class stub:
```java
public final class CoinbaseL2SnapshotStrategy extends SubscribeDrivenSnapshotStrategy {
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SnapshotStrategyTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2RecoveryStrategyTest.java`

Step-by-step:
1. Verify gate opens only after valid snapshot acceptance.
2. Verify updates before first snapshot remain dropped.
3. Verify recovery stub calls restored callback and does not perform reconnect flow.

### 9. Out of Scope
- Full reconnect/resubscribe
- BOOK_RESET/recovery ordering
- Heartbeat-triggered recovery action

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-005

### 1. Task ID
`P2-005`

### 2. Task Name
Coinbase L2 Connector and Factory Wiring

### 3. Task Description
Implement the production `COINBASE_L2` connector and connector factory by extending the shared core abstractions from Phase 1. This card owns the assembly of parser, snapshot strategy, recovery stub, subscription builder, transport, counters, clocks, and typed venue config into a working connector for one instrument.

### 4. Spec References
- Sections 5.1, 5.2, 8.1, 8.2, 10, 14, 25, 26
- AC-1, AC-8, AC-10, AC-12, AC-17, AC-18, AC-52, AC-62

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 5.1, 5.2, 8.1, 8.2, 10, 14, 25, 26
- outputs from P2-000 through P2-004
- Phase 1 connector base abstractions and bootstrap

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2Connector.java`
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorFactory.java`
- `src/main/resources/META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory`

Related files:
- `src/main/java/io/rueishi/marketdata/crypto/core/connector/AbstractConnector.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/connector/AbstractConnectorFactory.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/...`

Do not modify unless necessary:
- core recovery model beyond Phase 2 wiring

### 4b. Required Spec Extracts
- One connector per instrument.
- `AbstractConnector` owns parser, snapshot strategy, recovery strategy, subscription builder, and transport.
- Factory parses raw venue config to typed config and wires concrete venue components.
- `COINBASE_L2` package must be complete.

### 4c. Implementation Guidelines For This Task
- Keep concrete Coinbase L2 classes small and wiring-oriented.
- Let shared base classes own generic lifecycle behavior.
- Keep venue-specific logic in venue package classes only.

### 4d. Expanded Authoritative Spec Excerpts
- `ConnectorFactory` is registered via `ServiceLoader`.
- `CoinbaseL2ConnectorFactory` should parse `CoinbaseConfig`, construct venue components, and return one connector per instrument.
- `AbstractConnector` should not parse raw venue config itself.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-1, AC-8, AC-10, AC-12, AC-17, AC-18, AC-52, AC-62

Tests to add:
- `CoinbaseL2ConnectorFactoryTest`
- `CoinbaseL2ConnectorLifecycleTest`
- `CoinbaseL2ServiceLoaderTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: validated config, typed Coinbase venue config, and one instrument
- Output: fully wired `COINBASE_L2` connector ready to connect and publish
- Logic: connector/factory is the assembly point that turns shared infra into a working venue implementation

Edge path:
- Input: multiple instruments for same venue
- Output: one connector per instrument with isolated counters/transport/session state
- Logic: preserve per-instrument isolation

Negative path:
- Input: malformed venue config or missing factory registration
- Output: startup/factory failure before live connection
- Logic: fail early in wiring

Exception path:
- Input: component constructor failure
- Output: explicit init failure with partial resources not leaked
- Logic: connector creation should not half-succeed

Failure path:
- Input: venue logic leaks into `core`
- Output: architectural drift and harder Phase 4 extensibility
- Logic: keep the core/venue boundary clean

### 7. Implementation Steps
Classes/files:
- `venue/coinbase/l2/CoinbaseL2Connector.java`
- `venue/coinbase/l2/CoinbaseL2ConnectorFactory.java`
- service registration file

Step-by-step:
1. Implement `CoinbaseL2Connector` as a thin subclass of `AbstractConnector`.
2. Implement `CoinbaseL2ConnectorFactory` extending `AbstractConnectorFactory`.
3. Parse typed Coinbase venue config from raw venue config map.
4. Wire parser, snapshot strategy, recovery stub, subscription builder, transport, counters, and clocks.
5. Register factory in `META-INF/services`.

Class stub:
```java
public final class CoinbaseL2ConnectorFactory extends AbstractConnectorFactory {
    @Override
    public VenueEnum venue() { return VenueEnum.COINBASE_L2; }
}
```

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorFactoryTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorLifecycleTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ServiceLoaderTest.java`

Step-by-step:
1. Verify factory parses venue config and returns the correct venue enum.
2. Verify one connector is created per instrument with isolated state.
3. Verify `ServiceLoader` resolves `CoinbaseL2ConnectorFactory`.
4. Verify connector init/connect wiring uses injectable clocks and transport dependencies.

### 9. Out of Scope
- Full recovery lifecycle
- `COINBASE_L3`
- Graceful shutdown

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-006

### 1. Task ID
`P2-006`

### 2. Task Name
Phase 2 Counters and Liveness Monitoring

### 3. Task Description
Implement the live-path observability behavior Phase 2 explicitly owns: per-instrument counters, active-connection gauge updates, heartbeat receive/miss tracking, health-state updates, and liveness timeout detection without autonomous recovery action.

### 4. Spec References
- Sections 4.12, 6.6, 14, 15, 24, 25, 26
- AC-4, AC-41, AC-44, AC-45, AC-52

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 4.12, 6.6, 14, 15, 24, 25, 26
- outputs from P2-000, P2-001, and P2-005

Expected modified files:
- `src/main/java/io/rueishi/marketdata/crypto/core/observability/GatewayCounters.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/observability/InstrumentCounters.java`
- `src/main/java/io/rueishi/marketdata/crypto/core/connector/AbstractConnector.java`
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2Connector.java`

Related files:
- `src/test/java/io/rueishi/marketdata/crypto/core/observability/...`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2LivenessTest.java`

Do not modify unless necessary:
- Phase 3 metrics endpoint/persistence work
- autonomous recovery initiation

### 4b. Required Spec Extracts
- Heartbeat updates health state and observability counters but does not publish SBE.
- Active WebSocket connections gauge increments on establish and decrements on close.
- Heartbeat timeout detection is required in Phase 2, but recovery action is deferred.
- Time-dependent behavior must be injectable/testable.

### 4c. Implementation Guidelines For This Task
- Separate detection from action: Phase 2 may detect liveness failure and update counters/state only.
- Keep counter updates cheap and localized near the event that owns them.

### 4d. Expanded Authoritative Spec Excerpts
- `heartbeatsReceived`, `heartbeatsMissed`, and last-heartbeat timing are per-instrument observability signals.
- Liveness monitor starts only after successful ack validation to avoid a spurious early timeout.
- The transport does not own active-connection gauge updates or liveness scheduling; connector/observability code updates gauges from transport connection callbacks and schedules liveness after the parser reports successful ack validation.
- Phase 2 requires the live connector path counters, not the Prometheus endpoint.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-4, AC-41, AC-44, AC-45, AC-52

Tests to add:
- `CoinbaseL2LivenessTest`
- `Phase2ConnectionGaugeTest`
- `Phase2HeartbeatCountersTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: successful connect/ack and inbound heartbeat traffic
- Output: active-connection gauge updates from connector-owned connection callbacks; ack signal starts liveness; heartbeat health state and counters update correctly with no publish side effect
- Logic: Phase 2 observability is operational visibility for the live path

Edge path:
- Input: connect established before first heartbeat or ack
- Output: no false timeout before liveness monitor is legitimately active
- Logic: avoid subscribe-to-first-heartbeat false positives

Negative path:
- Input: stalled connection with no heartbeat beyond timeout
- Output: missed-heartbeat counters and health-state detection fire, but no full recovery action
- Logic: enforce Phase 2 boundary exactly

Exception path:
- Input: liveness scheduler callback throws
- Output: explicit failure handling without silently disabling monitoring
- Logic: hidden monitor failure is dangerous operationally

Failure path:
- Input: timeout detection starts recovery automatically
- Output: roadmap boundary violation
- Logic: keep detection only

### 7. Implementation Steps
Classes/files:
- `core/observability/GatewayCounters.java`
- `core/observability/InstrumentCounters.java`
- `core/connector/AbstractConnector.java`
- `venue/coinbase/l2/CoinbaseL2Connector.java`

Step-by-step:
1. Finalize heartbeat/liveness-related counters and gauge accessors.
2. Increment/decrement connection gauge in connector/observability code from transport establish/close callbacks.
3. Update heartbeat counters and last-seen timing from parser/connector callbacks.
4. Wire the parser ack-success signal to connector/session state and start liveness monitoring only after successful ack validation.
5. On timeout, update counters/health state only; do not invoke full recovery path.

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2LivenessTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/observability/Phase2ConnectionGaugeTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/core/observability/Phase2HeartbeatCountersTest.java`

Step-by-step:
1. Verify heartbeat frames update counters and no publish occurs.
2. Verify connection gauge increments/decrements in connector-owned callbacks on connect/close.
3. Verify ack-success signaling starts liveness monitoring, timeout detection is testable through injected time, and no recovery action is triggered.

### 9. Out of Scope
- Triggering recovery on timeout
- `/metrics`
- mmap persistence validation

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Task Card P2-007

### 1. Task ID
`P2-007`

### 2. Task Name
Phase 2 Fixture, Integration, and Structural Verification

### 3. Task Description
Implement the final Phase 2 verification layer: fixture-driven parser tests, end-to-end Coinbase L2 connector integration tests with `InMemoryPublisher`, package completeness checks, and ServiceLoader/structure validation.

### 4. Spec References
- Sections 3, 10, 14, 24, 25, 26

### 4a. Required Existing Inputs
Read these before implementing:
- `crypto_market_data_gateway_spec_v2.md` Sections 3, 10, 14, 24, 25, 26
- outputs from P2-001 through P2-006
- Phase 1 test scaffolding and `InMemoryPublisher`

Expected modified files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorIntegrationTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2PackageCompletenessTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2FixtureSuiteTest.java`
- `src/test/resources/fixtures/coinbase/l2/...`

Related files:
- `src/main/resources/META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory`
- `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/...`

Do not modify unless necessary:
- Phase 3 recovery tests
- Phase 4 L3 test packages

### 4b. Required Spec Extracts
- Phase 2 requires fixture-driven parser tests against all Coinbase L2 fixture files.
- `COINBASE_L2` sub-package must contain complete implementations.
- All Phase 1 and Phase 2 tests use `InMemoryPublisher`.
- Phase 2 integration verification must cover AC-48's platform-aware transport
  behavior: epoll/OpenSSL hooks when available and portable fallback otherwise.

### 4c. Implementation Guidelines For This Task
- Prefer deterministic fixture suites over ad hoc integration sleeps.
- Verify both byte-level output and package/service completeness.

### 4d. Expanded Authoritative Spec Excerpts
- Coinbase L2 fixtures should cover snapshot, l2update, heartbeat, subscriptions ack, malformed, and unknown-type paths.
- Integration tests must verify encoded output via `InMemoryPublisher`/test decoder rather than log-only inspection.

### 5. Acceptance Criteria and Tests
Acceptance criteria covered:
- AC-1 through AC-10 integration verification
- AC-12, AC-15 through AC-18 integration verification
- AC-41, AC-44, AC-45 integration verification
- AC-48, AC-52 integration verification
- AC-53 through AC-60 integration verification
- AC-62

Tests to add:
- `CoinbaseL2ConnectorIntegrationTest`
- `CoinbaseL2PackageCompletenessTest`
- `CoinbaseL2FixtureSuiteTest`

### 6. INPUT -> OUTPUT Paths
Positive path:
- Input: complete Coinbase L2 implementation and fixtures
- Output: deterministic proof that the full Phase 2 path works end to end
- Logic: this card validates the phase as a whole

Edge path:
- Input: malformed or unknown fixtures
- Output: counted/drop behavior verified without pipeline failure
- Logic: integration should cover more than only the happy path

Negative path:
- Input: missing Coinbase L2 class or bad service registration
- Output: structural/package test failure
- Logic: package completeness is itself a Phase 2 acceptance criterion

Exception path:
- Input: fixture mismatch or decoder assertion failure
- Output: precise failing test tied to the violated AC
- Logic: make final-phase failures actionable

Failure path:
- Input: relying only on unit tests without full connector integration
- Output: hidden wiring bugs survive until later phases
- Logic: Phase 2 must prove the working live-path assembly

### 7. Implementation Steps
Classes/files:
- integration/fixture/package-completeness tests
- fixture resources

Step-by-step:
1. Add or normalize Coinbase L2 fixture resources required by the spec.
2. Implement fixture suite tests covering supported, malformed, and unknown-type paths.
3. Implement end-to-end connector integration tests using `InMemoryPublisher` and decoder inspection.
4. Implement structural/package completeness test for `venue/coinbase/l2`.
5. Verify service registration and startup wiring resolve `COINBASE_L2` end to end.
6. Verify bootstrap event-loop selection uses epoll when available and NIO fallback otherwise.

### 8. Test Implementation Steps
Test classes/files:
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorIntegrationTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2PackageCompletenessTest.java`
- `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2FixtureSuiteTest.java`

Step-by-step:
1. Assert happy-path connect/auth/subscribe/parse/publish flow via mocks or embedded server.
2. Assert wire-format fields and timestamps via decoder inspection.
3. Assert package/service completeness and class presence.
4. Assert counter and liveness behavior appears in integration.

### 9. Out of Scope
- Phase 3 recovery integration
- `/metrics`
- `COINBASE_L3`

### 10. Completion Checklist Template
- Files changed:
- Tests added:
- ACs verified:
- Assumptions:
- Blockers:
- Follow-up items:

---

## Phase 2 Done Definition

Phase 2 is complete only when all cards above are complete and the following are true:
- `COINBASE_L2` connects, authenticates, subscribes, parses, encodes, and publishes through the shared runtime
- Phase 2 acceptance criteria all pass
- `mvn test` passes for Phase 1 and Phase 2 suites
- no Phase 3 recovery, metrics endpoint, graceful-shutdown, or latency work was pulled into Phase 2
- the implementation remains extensible for later phases without rewriting `core/`

## Cross-Card Notes

1. Phase 2 must reuse Phase 1 outputs rather than redefining core contracts unless a specific Phase 2 card explicitly owns a shared contract upgrade.
2. `CoinbaseL2RecoveryStrategy` remains a stub in this phase; do not silently implement the Phase 3 reconnect/resubscribe lifecycle.
3. Phase 2 observability is limited to counters and gauges required by live connector behavior.
4. `InMemoryPublisher` remains the required publisher for all Phase 2 tests.
5. Wire-format ACs `AC-57` through `AC-60` must be interpreted for `COINBASE_L2` only in this phase, using the `BOOK_LEVEL` template.

## Ownership Matrix

### Task Card Traceability Matrix

| Task Card | ACs | Spec Sections | Source Files | Test Files |
|---|---|---|---|---|
| `P2-000` | AC-4, AC-5, AC-6, AC-7, AC-9, AC-41, AC-44, AC-45, AC-52 support ownership | 6.6, 9.1-9.3, 14, 20, 24, 26 | `venue/coinbase/shared/CoinbaseConfig.java`, `venue/coinbase/shared/CoinbaseAuthenticator.java`, `core/observability/GatewayCounters.java`, `core/observability/InstrumentCounters.java` | `CoinbaseConfigTest`, `CoinbaseAuthenticatorTest`, `Phase2InstrumentCountersTest` |
| `P2-001` | AC-1, AC-10, AC-48, AC-52 | 3.1, 4.6, 5.3, 5.8, 14, 15, 18, 26 | `core/transport/NettyWebSocketTransport.java`, `core/transport/WebSocketFrameHandler.java`, `core/transport/PipelineCustomizer.java` | `NettyWebSocketTransportTest`, `NettyWebSocketTransportLinuxConfigTest`, `NettyWebSocketTransportClockInjectionTest` |
| `P2-002` | AC-9 support, AC-10 | 5.4, 14, 24, 25, 26 | `venue/coinbase/l2/CoinbaseL2SubscriptionBuilder.java`, `core/subscription/SubscriptionBuilder.java` | `CoinbaseL2SubscriptionBuilderTest` |
| `P2-003` | AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-9, AC-12, AC-15, AC-16, AC-17, AC-18, AC-53, AC-54, AC-55, AC-56, AC-57, AC-58, AC-59, AC-60 | 4.6, 5.3, 14, 16, 17, 24, 25, 26 | `venue/coinbase/l2/CoinbaseL2FeedParser.java`, `core/parser/FeedParser.java`, `core/parser/ParseContext.java`, `core/parser/DefaultParseContext.java` | `CoinbaseL2FeedParserTest`, `CoinbaseL2SubscriptionsAckTest`, `CoinbaseL2HeartbeatTest` |
| `P2-004` | AC-12, AC-15, AC-16 | 5.5, 5.6, 8.3, 8.4, 14, 25, 26 | `venue/coinbase/l2/CoinbaseL2SnapshotStrategy.java`, `venue/coinbase/l2/CoinbaseL2RecoveryStrategy.java` | `CoinbaseL2SnapshotStrategyTest`, `CoinbaseL2RecoveryStrategyTest` |
| `P2-005` | AC-1, AC-8, AC-10, AC-12, AC-17, AC-18, AC-52, AC-62 | 5.1, 5.2, 8.1, 8.2, 10, 14, 25, 26 | `venue/coinbase/l2/CoinbaseL2Connector.java`, `venue/coinbase/l2/CoinbaseL2ConnectorFactory.java`, `META-INF/services/...ConnectorFactory` | `CoinbaseL2ConnectorFactoryTest`, `CoinbaseL2ConnectorLifecycleTest`, `CoinbaseL2ServiceLoaderTest` |
| `P2-006` | AC-4, AC-41, AC-44, AC-45, AC-52 | 4.12, 6.6, 14, 15, 24, 25, 26 | `core/observability/GatewayCounters.java`, `core/observability/InstrumentCounters.java`, `core/connector/AbstractConnector.java`, `venue/coinbase/l2/CoinbaseL2Connector.java` | `CoinbaseL2LivenessTest`, `Phase2ConnectionGaugeTest`, `Phase2HeartbeatCountersTest` |
| `P2-007` | AC-1 through AC-10 integration verification, AC-12, AC-15 through AC-18, AC-41, AC-44, AC-45, AC-48, AC-52, AC-53 through AC-60, AC-62 | 3, 10, 14, 24, 25, 26 | test-only fixture resources and verification files | `CoinbaseL2ConnectorIntegrationTest`, `CoinbaseL2PackageCompletenessTest`, `CoinbaseL2FixtureSuiteTest` |

### Phase 2 AC Ownership

| AC | Owner Task Card |
|---|---|
| AC-1 | `P2-001`, `P2-005`, `P2-007` |
| AC-2 | `P2-003`, `P2-007` |
| AC-3 | `P2-003`, `P2-007` |
| AC-4 | `P2-003`, `P2-006`, `P2-007` |
| AC-5 | `P2-003`, `P2-007` |
| AC-6 | `P2-003`, `P2-007` |
| AC-7 | `P2-003`, `P2-007` |
| AC-8 | `P2-003`, `P2-005`, `P2-007` |
| AC-9 | `P2-002`, `P2-003`, `P2-007` |
| AC-10 | `P2-001`, `P2-002`, `P2-005`, `P2-007` |
| AC-12 | `P2-003`, `P2-004`, `P2-005`, `P2-007` |
| AC-15 | `P2-003`, `P2-004`, `P2-007` |
| AC-16 | `P2-003`, `P2-004`, `P2-007` |
| AC-17 | `P2-003`, `P2-005`, `P2-007` |
| AC-18 | `P2-003`, `P2-005`, `P2-007` |
| AC-41 | `P2-000`, `P2-006`, `P2-007` |
| AC-44 | `P2-000`, `P2-006`, `P2-007` |
| AC-45 | `P2-000`, `P2-006`, `P2-007` |
| AC-48 | `P2-001`, `P2-007` |
| AC-52 | `P2-000`, `P2-001`, `P2-005`, `P2-006`, `P2-007` |
| AC-53 | `P2-003`, `P2-007` |
| AC-54 | `P2-003`, `P2-007` |
| AC-55 | `P2-003`, `P2-007` |
| AC-56 | `P2-003`, `P2-007` |
| AC-57 | `P2-003`, `P2-007` |
| AC-58 | `P2-003`, `P2-007` |
| AC-59 | `P2-003`, `P2-007` |
| AC-60 | `P2-003`, `P2-007` |
| AC-62 | `P2-005`, `P2-007` |

### Phase 2 Spec Section Ownership

| Spec Section | Owner Task Card |
|---|---|
| 3.1 Maven Layout | `P2-001`, `P2-007` |
| 4.6 Parser Layer | `P2-001`, `P2-003` |
| 4.12 Observability | `P2-000`, `P2-006` |
| 5.1 Connector | `P2-005` |
| 5.2 ConnectorFactory | `P2-005` |
| 5.3 FeedParser | `P2-001`, `P2-003` |
| 5.4 SubscriptionBuilder | `P2-002` |
| 5.5 SnapshotStrategy | `P2-004` |
| 5.6 RecoveryStrategy | `P2-004` |
| 5.8 PipelineCustomizer | `P2-001` |
| 6.6 GatewayCounters | `P2-000`, `P2-006` |
| 8.1 AbstractConnector | `P2-005`, `P2-006` |
| 8.2 AbstractConnectorFactory | `P2-005` |
| 8.3 SubscribeDrivenSnapshotStrategy | `P2-004` |
| 8.4 ReconnectRecoveryStrategy | `P2-004` |
| 9.1-9.3 Configuration | `P2-000` |
| 10 Startup Sequence and Registry | `P2-005`, `P2-007` |
| 12 Buffer and Memory Model | `P2-003` |
| 13 Parser Strategy | `P2-003` |
| 14 Coinbase L2 Strategy | `P2-000` through `P2-007` as applicable |
| 15 Threading Model | `P2-001`, `P2-006` |
| 16 Normalized Binary Model | `P2-003` |
| 17 Binary Encoding Model | `P2-003` |
| 18 Low-Latency Guidelines | `P2-001`, `P2-003` |
| 20 Publisher Model | `P2-000`, `P2-003` |
| 24 Test Strategy | `P2-000` through `P2-007` as applicable |
| 25 Acceptance Criteria | All owning cards listed in the AC matrix |
| 26 Phase 2 Roadmap | `P2-000` through `P2-007` |

### Phase 2 Source Class Ownership

| Source Class / File | Owner Task Card |
|---|---|
| `core/transport/NettyWebSocketTransport.java` | `P2-001` |
| `core/transport/WebSocketFrameHandler.java` | `P2-001` |
| `core/transport/PipelineCustomizer.java` | `P2-001` |
| `core/subscription/SubscriptionBuilder.java` | `P2-002` |
| `core/parser/FeedParser.java` | `P2-003` |
| `core/parser/ParseContext.java` | `P2-003`, `P2-006` |
| `core/parser/DefaultParseContext.java` | `P2-003`, `P2-006` |
| `core/observability/GatewayCounters.java` | `P2-000`, `P2-006` |
| `core/observability/InstrumentCounters.java` | `P2-000`, `P2-006` |
| `core/connector/AbstractConnector.java` | `P2-005`, `P2-006` |
| `venue/coinbase/shared/CoinbaseConfig.java` | `P2-000` |
| `venue/coinbase/shared/CoinbaseAuthenticator.java` | `P2-000` |
| `venue/coinbase/l2/CoinbaseL2SubscriptionBuilder.java` | `P2-002` |
| `venue/coinbase/l2/CoinbaseL2FeedParser.java` | `P2-003` |
| `venue/coinbase/l2/CoinbaseL2SnapshotStrategy.java` | `P2-004` |
| `venue/coinbase/l2/CoinbaseL2RecoveryStrategy.java` | `P2-004` |
| `venue/coinbase/l2/CoinbaseL2Connector.java` | `P2-005`, `P2-006` |
| `venue/coinbase/l2/CoinbaseL2ConnectorFactory.java` | `P2-005` |
| `src/main/resources/META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory` | `P2-005` |

### Phase 2 Test Class Ownership

| Test Class / File | Owner Task Card |
|---|---|
| `CoinbaseConfigTest` | `P2-000` |
| `CoinbaseAuthenticatorTest` | `P2-000` |
| `Phase2InstrumentCountersTest` | `P2-000` |
| `NettyWebSocketTransportTest` | `P2-001` |
| `NettyWebSocketTransportLinuxConfigTest` | `P2-001` |
| `NettyWebSocketTransportClockInjectionTest` | `P2-001` |
| `CoinbaseL2SubscriptionBuilderTest` | `P2-002` |
| `CoinbaseL2FeedParserTest` | `P2-003` |
| `CoinbaseL2SubscriptionsAckTest` | `P2-003` |
| `CoinbaseL2HeartbeatTest` | `P2-003` |
| `CoinbaseL2SnapshotStrategyTest` | `P2-004` |
| `CoinbaseL2RecoveryStrategyTest` | `P2-004` |
| `CoinbaseL2ConnectorFactoryTest` | `P2-005` |
| `CoinbaseL2ConnectorLifecycleTest` | `P2-005` |
| `CoinbaseL2ServiceLoaderTest` | `P2-005` |
| `CoinbaseL2LivenessTest` | `P2-006` |
| `Phase2ConnectionGaugeTest` | `P2-006` |
| `Phase2HeartbeatCountersTest` | `P2-006` |
| `CoinbaseL2ConnectorIntegrationTest` | `P2-007` |
| `CoinbaseL2PackageCompletenessTest` | `P2-007` |
| `CoinbaseL2FixtureSuiteTest` | `P2-007` |

### Phase 2 Folder Structure Ownership

| Folder / Structure | Owner Task Card |
|---|---|
| `src/main/java/io/rueishi/marketdata/crypto/core/transport/` | `P2-001` |
| `src/main/java/io/rueishi/marketdata/crypto/core/subscription/` | `P2-002` |
| `src/main/java/io/rueishi/marketdata/crypto/core/parser/` | `P2-003` |
| `src/main/java/io/rueishi/marketdata/crypto/core/observability/` | `P2-000`, `P2-006` |
| `src/main/java/io/rueishi/marketdata/crypto/core/connector/` | `P2-005`, `P2-006` |
| `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/` | `P2-000` |
| `src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/` | `P2-002`, `P2-003`, `P2-004`, `P2-005`, `P2-006` |
| `src/main/resources/META-INF/services/` | `P2-005` |
| `src/test/java/io/rueishi/marketdata/crypto/core/transport/` | `P2-001` |
| `src/test/java/io/rueishi/marketdata/crypto/core/observability/` | `P2-000`, `P2-006` |
| `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/shared/` | `P2-000` |
| `src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/` | `P2-002`, `P2-003`, `P2-004`, `P2-005`, `P2-006`, `P2-007` |
| `src/test/resources/fixtures/coinbase/l2/` | `P2-007` |

### Ownership Rule

If any new Phase 2 file, helper, test utility, or folder is introduced during implementation and it is not already listed above, the developer must:
1. assign it to an existing Phase 2 task card before implementation, or
2. create a narrowly scoped addendum task card and update this matrix.

## Verification Summary

- Completeness status: The plan covers every Phase 2 AC and assigns ownership for the major Phase 2 source files, tests, and folders required by the roadmap.
- Ambiguities found: No blocking Phase 2 ambiguity remains after the recent spec cleanup. The only ongoing caution is that `AC-57` through `AC-60` must be implemented as `COINBASE_L2`/`BOOK_LEVEL` proof in this phase, not as L3 work.
- Assumptions made: Phase 1 outputs exist and remain stable enough to reuse without redesign; `InMemoryPublisher` remains the Phase 2 test publisher; transport tests may use mocks or embedded servers rather than live Coinbase.
- Recommended spec fixes: None required before Phase 2 implementation planning.
- Confidence assessment: High for planning structure and ownership. Medium-high for exact file granularity because implementation may still discover a few helper types.
