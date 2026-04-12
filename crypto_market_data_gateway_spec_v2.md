# Crypto Market Data Gateway — Specification v2

> **Low Latency** | **Off-Heap** | **Recovery-Aware** | **Explicit Ownership** | **Multi-Venue Extensible**
>
> This document is the single authoritative specification. All prior drafts,
> amendments, and review files are superseded by this file.

---

## 1. System Goal

Design and implement a **low-latency, low-allocation crypto market data gateway**
in **Java 21 (Maven)** that:

- Supports multiple exchange venues through a clean per-venue package structure
- Runs as a **single-venue-per-instance** process — one instance, one exchange,
  one configured list of instruments
- Maintains **one WebSocket connection per configured instrument**
- Parses exchange-native JSON directly from **Netty off-heap direct `ByteBuf`**
- Normalizes exchange-specific semantics while encoding
- Encodes directly into a **compact SBE-style binary format** over an
  **Agrona off-heap direct buffer**
- Exposes publisher and recovery interfaces for downstream consumers
- Detects feed-integrity and liveness failures and self-triggers recovery
- Supports downstream-driven recovery via explicit recovery interfaces

The **initial implementation** targets the **Coinbase Exchange Direct Feed**
— the authenticated institutional WebSocket API at
`wss://ws-feed.exchange.coinbase.com`. All auth, message shapes, and channel
names in Section 14 refer to the Exchange Direct Feed exclusively. The
architecture is designed so additional venues (Binance, Kraken, OKX, etc.)
are added as self-contained packages with no changes to core gateway logic.

The system prioritizes:
- **Deterministic latency**
- **Minimal garbage generation**
- **Explicit architecture and buffer ownership**
- **Off-heap hot-path processing**

---

## 2. Architecture

### 2.1 High-Level Flow

```
Netty WebSocket Transport (direct pooled ByteBuf, off-heap)
    →
Frame Handling (TextWebSocketFrame / BinaryWebSocketFrame → ByteBuf)
    →
Venue FeedParser (low-allocation scanning directly over ByteBuf)
    →
SbeEncoder (direct write into reusable Agrona MutableDirectBuffer, off-heap)
    →
Publisher (handoff — no copy unless required by downstream transport)
    →
Downstream Integration Boundary
```

### 2.2 Design Principles

- **Single-venue-per-instance**: one running process serves one exchange
- **One connection per instrument**: complete failure isolation per instrument
- **Single-pass processing**: parse → encode in one pass over the ByteBuf
- **No intermediate object model on hot path**: exchange fields written directly into SBE binary
- **No reflection, no generic JSON mapping**
- **Explicit off-heap buffer ownership**
- **Thread-affine processing**: event loop owns its connector, buffer, and context objects
- **Venue implementations are stateless**: all session state lives in context objects owned by the connector

---

## 3. Project Structure

### 3.1 Maven Layout

```
crypto-market-data-gateway/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/io/rueishi/marketdata/crypto/
    │   │   ├── core/
    │   │   │   ├── bootstrap/
    │   │   │   │   ├── GatewayBootstrap.java
    │   │   │   │   └── VenueRegistry.java           # ServiceLoader — no venue imports
    │   │   │   ├── config/
    │   │   │   │   ├── GatewayConfig.java
    │   │   │   │   ├── InstrumentConfig.java
    │   │   │   │   ├── TransportConfig.java
    │   │   │   │   ├── EncodingConfig.java
    │   │   │   │   ├── ObservabilityConfig.java
    │   │   │   │   ├── PublisherConfig.java           # type + per-type sub-config
    │   │   │   │   ├── PublisherType.java             # LOGGING, IN_MEMORY, SHARED_MEMORY, AERON, CHRONICLE
    │   │   │   │   ├── LoggingPublisherConfig.java    # outputPath, rollSizeMb
    │   │   │   │   ├── VenueEnum.java                 # Exchange + data level as atomic identity
    │   │   │   │   ├── BookDepth.java                 # L1=1, L2=2, L3=3
    │   │   │   │   └── TemplateId.java                # BOOK_LEVEL=1, ORDER_ENTRY=2
    │   │   │   ├── connector/
    │   │   │   │   ├── Connector.java               # Interface
    │   │   │   │   ├── ConnectorFactory.java         # Interface (ServiceLoader SPI)
    │   │   │   │   ├── ConnectorContext.java         # Interface
    │   │   │   │   ├── AbstractConnector.java        # Base — all lifecycle + context mgmt
    │   │   │   │   └── AbstractConnectorFactory.java # Base — config parsing + wiring
    │   │   │   ├── transport/
    │   │   │   │   ├── NettyWebSocketTransport.java  # Shared base transport
    │   │   │   │   ├── WebSocketFrameHandler.java    # Interface: onTextFrame, onBinaryFrame, onDisconnected
    │   │   │   │   └── PipelineCustomizer.java       # Interface
    │   │   │   ├── parser/
    │   │   │   │   ├── FeedParser.java               # Interface
    │   │   │   │   ├── ParseContext.java             # Interface
    │   │   │   │   ├── DefaultParseContext.java
    │   │   │   │   └── ByteBufScanner.java           # Shared: scan, decimal, timestamp utils
    │   │   │   ├── subscription/
    │   │   │   │   └── SubscriptionBuilder.java      # Interface
    │   │   │   ├── snapshot/
    │   │   │   │   ├── SnapshotStrategy.java         # Interface
    │   │   │   │   ├── SnapshotContext.java           # Interface
    │   │   │   │   ├── DefaultSnapshotContext.java
    │   │   │   │   ├── SnapshotGatekeeper.java       # Concrete class
    │   │   │   │   └── SubscribeDrivenSnapshotStrategy.java # Base — no-op triggerSnapshot
    │   │   │   ├── recovery/
    │   │   │   │   ├── RecoveryStrategy.java         # Interface
    │   │   │   │   ├── RecoveryContext.java           # Interface
    │   │   │   │   ├── DefaultRecoveryContext.java
    │   │   │   │   ├── RecoveryRequest.java
    │   │   │   │   ├── RecoveryReasonCode.java
    │   │   │   │   └── ReconnectRecoveryStrategy.java # Base — disconnect/reconnect template
    │   │   │   ├── encoding/
    │   │   │   │   └── SbeEncoder.java               # Concrete — one per connector
    │   │   │   ├── sequence/
    │   │   │   │   └── SequenceTracker.java           # Concrete class
    │   │   │   ├── publisher/
    │   │   │   │   ├── Publisher.java                # Interface
    │   │   │   │   ├── InMemoryPublisher.java        # Test publisher — pre-allocated ring capture
    │   │   │   │   └── LoggingPublisher.java         # Log4j 2 async integration validation adapter
    │   │   │   └── observability/
    │   │   │       ├── GatewayCounters.java          # Counter facade — allocates InstrumentCounters
    │   │   │       ├── InstrumentCounters.java        # Per-instrument Agrona AtomicCounter set
    │   │   │       └── MetricsEndpoint.java
    │   │   │
    │   │   └── venue/
    │   │       │   # Each venue folder has:
    │   │       │   #   l2/      — L2 aggregated depth strategy (if supported)
    │   │       │   #   l3/      — L3 individual order strategy (if supported)
    │   │       │   #   shared/  — auth, config, endpoint constants shared across levels
    │   │       │   #
    │   │       │   # Venue developers write only the classes specific to their exchange.
    │   │       │   # All lifecycle, context management, and recovery boilerplate
    │   │       │   # lives in core/ base classes — see Section 8.
    │   │       │
    │   │       ├── coinbase/
    │   │       │   ├── l2/                          # COINBASE_L2 strategy
    │   │       │   │   ├── CoinbaseL2Connector.java        # ~10 lines — extends AbstractConnector
    │   │       │   │   ├── CoinbaseL2ConnectorFactory.java # ~20 lines — extends AbstractConnectorFactory
    │   │       │   │   ├── CoinbaseL2FeedParser.java        # ~80 lines — genuine parse logic
    │   │       │   │   ├── CoinbaseL2SnapshotStrategy.java  # ~3 lines  — extends SubscribeDrivenSnapshotStrategy
    │   │       │   │   ├── CoinbaseL2RecoveryStrategy.java  # ~20 lines — extends ReconnectRecoveryStrategy
    │   │       │   │   ├── CoinbaseL2SubscriptionBuilder.java # ~30 lines — genuine auth+JSON logic
    │   │       │   │   └── README.md
    │   │       │   ├── l3/                          # COINBASE_L3 strategy
    │   │       │   │   ├── CoinbaseL3Connector.java
    │   │       │   │   ├── CoinbaseL3ConnectorFactory.java
    │   │       │   │   ├── CoinbaseL3FeedParser.java
    │   │       │   │   ├── CoinbaseL3SnapshotStrategy.java  # REST_THEN_DELTA
    │   │       │   │   ├── CoinbaseL3RecoveryStrategy.java
    │   │       │   │   ├── CoinbaseL3SubscriptionBuilder.java # subscribes to "full"
    │   │       │   │   └── README.md
    │   │       │   └── shared/                      # shared auth and config
    │   │       │       ├── CoinbaseAuthenticator.java
    │   │       │       └── CoinbaseConfig.java
    │   │       │
    │   │       ├── binance/                         # Future
    │   │       │   ├── l2/
    │   │       │   │   └── README.md                # BINANCE_L2 strategy
    │   │       │   └── shared/
    │   │       │
    │   │       └── kraken/                          # Future
    │   │           ├── l2/
    │   │           │   └── README.md                # KRAKEN_L2 strategy
    │   │           └── shared/
    │   │
    │   └── resources/
    │       └── META-INF/services/
    │           └── io.rueishi.marketdata.crypto.core.connector.ConnectorFactory
    │               # One line per registered venue+level strategy:
    │               # io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2ConnectorFactory
    │               # io.rueishi.marketdata.crypto.venue.coinbase.l3.CoinbaseL3ConnectorFactory
    └── test/
        ├── java/io/rueishi/marketdata/crypto/
        │   ├── core/
        │   │   ├── encoding/                        # SbeEncoderTest (L2 + L3 templates)
        │   │   │                                    # SbeDecoder   — test-only decoder utility
        │   │   ├── sequence/                        # SequenceTrackerTest
        │   │   ├── snapshot/                        # SnapshotGatekeeperTest
        │   │   ├── recovery/                        # RecoveryCoalescingTest
        │   │   ├── parser/                          # ByteBufScannerTest
        │   │   └── bootstrap/                       # VenueRegistryTest
        │   └── venue/
        │       └── coinbase/
        │           ├── l2/
        │           │   ├── CoinbaseL2FeedParserTest.java
        │           │   ├── CoinbaseL2SnapshotStrategyTest.java
        │           │   ├── CoinbaseL2RecoveryStrategyTest.java
        │           │   └── CoinbaseL2ConnectorIntegrationTest.java
        │           └── l3/
        │               ├── CoinbaseL3FeedParserTest.java
        │               ├── CoinbaseL3SnapshotStrategyTest.java
        │               ├── CoinbaseL3RecoveryStrategyTest.java
        │               └── CoinbaseL3ConnectorIntegrationTest.java
        └── resources/
            └── venue/
                └── coinbase/
                    ├── l2/
                    │   ├── snapshot.json
                    │   ├── l2update.json
                    │   ├── heartbeat.json
                    │   ├── subscriptions_ack.json
                    │   ├── malformed.json
                    │   └── unknown_type.json
                    └── l3/
                        ├── snapshot_rest.json
                        ├── received.json
                        ├── open.json
                        ├── done.json
                        ├── match.json
                        ├── change.json
                        ├── malformed.json
                        └── unknown_type.json
```

### 3.2 Package Rules

- **`core/`** contains zero exchange-specific logic. It must not import from
  any `venue/` package. Enforced by an ArchUnit structural test (see AC-61).
- **`venue/<n>/`** contains all exchange-specific logic. It imports from
  `core/` only.
- **`VenueRegistry`** discovers venue factories via Java `ServiceLoader`.
  It never names a venue class directly. This is the only connection between
  `core/` and `venue/` at runtime.
- **No hot-path code dispatches on venue.** The venue is fixed at startup
  from configuration.

---

## 4. Components

### 4.1 Gateway Instance

- One running process, one configuration file, **exactly one exchange venue**
- Manages one `Connector` per configured instrument
- At startup: reads config → discovers venue factory via `VenueRegistry` →
  creates one `Connector` per instrument → connects all
- Downstream recovery requests are validated against the configured venue at
  the gateway boundary, then routed to the matching connector by `instrumentId`

### 4.2 Transport Layer

- Netty-based WebSocket client with TLS (`wss://`) support
- Uses **pooled direct off-heap `ByteBuf`** via `PooledByteBufAllocator`
- Shared base class `NettyWebSocketTransport` handles: TLS handshake,
  WebSocket upgrade, ping/pong, reconnect with exponential backoff,
  graceful shutdown
- Venue connectors supply a `PipelineCustomizer` for venue-specific handlers
  (e.g. per-message deflate for binary-frame venues)
- Enforces per-connector heartbeat/liveness timeout
- Supports `SIGTERM` graceful shutdown

#### `NettyWebSocketTransport` API Contract

`NettyWebSocketTransport` is a concrete class in `core/transport/`. Venue
connectors that extend `AbstractConnector` delegate transport operations to it.
The class uses a caller-supplied `EventLoopGroup` (one thread per connector)
and owns the Netty `Bootstrap` plus channel lifecycle.

```java
// core/transport/NettyWebSocketTransport.java
public class NettyWebSocketTransport {

    /**
     * Constructs the transport. Does not connect.
     *
     * @param eventLoopGroup   caller-owned Netty event loop group for this connector
     * @param uri              WebSocket endpoint (e.g. wss://ws-feed.exchange.coinbase.com)
     * @param sslContext       pre-built BoringSSL SslContext (SslProvider.OPENSSL)
     * @param customizer       venue-specific Netty pipeline additions (no-op for most venues)
     * @param frameSizeLimit   max inbound frame size in bytes (from config)
     * @param connectTimeoutMs TCP connect + TLS + WS upgrade deadline in milliseconds
     *                         (from TransportConfig.connectTimeoutMs). Applied via
     *                         ChannelOption.CONNECT_TIMEOUT_MILLIS on the Bootstrap.
     * @param frameHandler     callback for parsed WebSocket frames, called on event-loop thread
     */
    public NettyWebSocketTransport(EventLoopGroup eventLoopGroup,
                                   URI uri, SslContext sslContext,
                                   PipelineCustomizer customizer,
                                   int frameSizeLimit,
                                   int connectTimeoutMs,
                                   WebSocketFrameHandler frameHandler) { ... }

    /**
     * Dials the connection synchronously: TCP connect → TLS handshake →
     * WebSocket upgrade. Blocks the calling thread until fully connected or
     * throws on timeout/error. Must be called from the event-loop thread or
     * before event-loop is pinned.
     *
     * The connect timeout is applied via ChannelOption.CONNECT_TIMEOUT_MILLIS
     * set at Bootstrap construction time (from the connectTimeoutMs constructor
     * parameter). If the deadline elapses before the channel is active,
     * Netty closes the channel and the ChannelFuture fails with a
     * ConnectTimeoutException, which this method wraps and rethrows as IOException.
     *
     * @throws IOException on connection failure or timeout
     */
    public void connect() throws IOException { ... }

    /**
     * Sends a pre-encoded UTF-8 JSON text frame. Non-blocking.
     * Must be called from the owning event-loop thread.
     * Throws IllegalStateException if not connected.
     */
    public void sendText(byte[] utf8Payload) { ... }

    /**
     * Closes the connection cleanly: sends a WebSocket Close frame,
     * waits up to closeTimeoutMs, then force-closes. Non-blocking;
     * completion is eventual.
     */
    public void close() { ... }

    /**
     * Returns the Netty Channel's event loop. Used by AbstractConnector
     * to schedule recovery signals onto this connector's thread.
     */
    public EventLoop eventLoop() { ... }

    /**
     * Returns true if the WebSocket channel is currently active (connected
     * and not closing). Thread-safe — may be called from any thread.
     */
    public boolean isConnected() { ... }
}
```

**`WebSocketFrameHandler` — callback interface:**

```java
// core/transport/WebSocketFrameHandler.java
public interface WebSocketFrameHandler {
    /** Called on the event-loop thread for each inbound text frame. */
    void onTextFrame(ByteBuf frame);

    /** Called on the event-loop thread for each inbound binary frame. */
    void onBinaryFrame(ByteBuf frame);

    /** Called on the event-loop thread when the connection is lost unexpectedly. */
    void onDisconnected(Throwable cause);
}
```

`AbstractConnector` implements `WebSocketFrameHandler` and passes `this` to the
transport constructor. `onTextFrame` and `onBinaryFrame` delegate directly to the
`FeedParser`. `onDisconnected` triggers the recovery flow.

**Reconnect responsibility:** `NettyWebSocketTransport` does NOT automatically
reconnect. `ReconnectRecoveryStrategy.doReconnect()` calls `transport.connect()`
explicitly. This keeps retry policy in the strategy, not the transport.

#### `EpollEventLoopGroup` Lifecycle

`GatewayBootstrap` creates exactly one Netty `EventLoopGroup` with one thread per
connector and passes it into `NettyWebSocketTransport`. On Linux, bootstrap must
prefer `EpollEventLoopGroup` when `Epoll.isAvailable()` returns true. When epoll
is unavailable, including non-Linux development and test environments, bootstrap
falls back to `NioEventLoopGroup` so the gateway remains portable. One connector
= one transport = one event-loop thread. Groups are not shared across connectors. This
is intentional — failure isolation, independent CPU pinning, and single-writer
discipline all require each connector to run on its own event loop.

```java
// Inside GatewayBootstrap startup loop:
EventLoopGroup eventLoopGroup = Epoll.isAvailable()
    ? new EpollEventLoopGroup(1, namedThreadFactory(instrument))
    : new NioEventLoopGroup(1, namedThreadFactory(instrument));
```

Lifecycle:
- **Created:** in `GatewayBootstrap` before `connector.init()`
- **Injected:** via `ConnectorContext.eventLoopGroup()` into `AbstractConnector.init()`,
  then into the `NettyWebSocketTransport` constructor
- **Active:** from `connect()` through the connector's lifetime
- **Transport close:** `NettyWebSocketTransport.close()` closes the channel only;
  it must NOT shut down the externally owned event-loop group
- **Shut down:** `GatewayBootstrap` calls
  `eventLoopGroup.shutdownGracefully(quietPeriodMs, timeoutMs, MILLISECONDS)`
  after `AbstractConnector.shutdown()` closes the transport channel and publishes
  `BOOK_RESET`
- **Shutdown timeout:** `quietPeriodMs = 100`, `timeoutMs = shutdownDeadlineMs / 2`.
  If the group does not terminate within the timeout, `awaitTermination()` returns
  and the gateway proceeds with shutdown regardless.

#### `SslContext` Lifecycle and Trust Manager

`SslContext` is built once per connector in `AbstractConnector.init()` via
`buildSslContext(ConnectorContext)`. It is not shared across connectors — each
connector builds its own. `SslContext` is immutable and internally thread-safe
for reuse across reconnects on the same connector.

**Production trust manager** — use the JDK default trust store:
```java
// Production — trusts CAs in the JDK cacerts bundle (includes Coinbase's CA)
protected SslContext buildSslContext(ConnectorContext ctx) throws SSLException {
    return SslContextBuilder.forClient()
        .sslProvider(SslProvider.OPENSSL)     // BoringSSL via netty-tcnative-boringssl-static
        // No .trustManager() call — uses JDK default TrustManagerFactory (JKS cacerts)
        .build();
}
```

**Sandbox / integration test trust manager** — override in the venue subclass:
```java
// Sandbox only — do NOT use in production
@Override
protected SslContext buildSslContext(ConnectorContext ctx) throws SSLException {
    return SslContextBuilder.forClient()
        .sslProvider(SslProvider.OPENSSL)
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build();
}
```

The `buildSslContext()` method is `protected` specifically so integration tests
can override it without modifying `AbstractConnector`.

### 4.3 Connector

- One per configured instrument
- Owns the full lifecycle: connect → subscribe → process frames → recover → shutdown
- Owns all context objects and manages their lifecycle precisely:
  - `DefaultParseContext` — created once in `init()`; `resetSession()` is called on
    recovery to renew session-scoped state in place; the object itself is never replaced
  - `DefaultSnapshotContext` — created once in `init()`; rebuilt on every recovery
    because it must point at the new `SnapshotGatekeeper` that `resetSession()` creates
  - `DefaultRecoveryContext` — rebuilt after every recovery attempt because its
    callbacks model a single attempt lifecycle
- Enforces single-writer access to the reusable encode buffer
- All hot-path and recovery work executes on the owning Netty event-loop thread
- Fixed fields: `SbeEncoder`, `Publisher`, `InstrumentCounters`, `NanoClock`
- Session-scoped state lives inside context objects, not as connector fields

### 4.4 ConnectorFactory

- One factory per venue, discovered at startup via Java `ServiceLoader`
- Creates all venue-specific components for one instrument and wires them
- Called once per instrument at startup — may allocate freely
- Returns a fully constructed `Connector` ready for `init(ConnectorContext)`

### 4.5 Subscription Builder

- Builds subscribe and unsubscribe JSON payloads for the venue
- Authentication payload generation is control-path work, not hot-path work
- Reuses pre-loaded credentials; avoids heap churn during signing
- Secrets must not appear in logs, metrics labels, or exception messages

### 4.6 Parser Layer

- One `FeedParser` implementation per venue, inside that venue's package
- Handwritten scanners over **Netty `ByteBuf`** — no library JSON parsing on hot path
- **Stateless** — holds no session state as fields; all state comes from `ParseContext`
- Supports text and binary WebSocket frames via `onTextFrame` / `onBinaryFrame`
- Responsible for sequence mapping: supplies `gatewayMessageSeq`, `seq1`, and
  `seq2` to `SbeEncoder.beginMessage()` using the venue-appropriate source

### 4.7 Snapshot Strategy

- Defines how the venue delivers the initial book snapshot after subscribe or recovery
- **`SUBSCRIBE_DRIVEN`**: snapshot arrives automatically on subscribe (e.g. Coinbase)
- **`REST_THEN_DELTA`**: snapshot fetched via REST; WS updates buffered and
  sequence-aligned before publishing (e.g. Binance)
- REST fetches use Java 11 `java.net.http.HttpClient` on a dedicated non-hot-path
  thread; result handed back to the connector via a task scheduled on the
  connector's event-loop thread
- Shares the same `SnapshotGatekeeper` instance as `ParseContext`

### 4.8 Recovery Strategy

- One implementation per venue
- Executes on the owning event-loop thread
- Signals completion to the connector via `RecoveryContext.onChannelRestored()`
  or `onRecoveryFailed()` — exactly one signal per `execute()` call

### 4.9 Encoding Layer

- No intermediate normalized event container on hot path
- Venue parser drives `SbeEncoder` directly:
  `beginMessage()` → `writeLevel()` × N → `endMessage(publisher, counters, nanoClock)`
- `entryCount` is tracked internally by `SbeEncoder` and patched into the
  header by `endMessage()` — callers never pre-count entries
- Writes into the reusable **Agrona `MutableDirectBuffer`** (off-heap)

### 4.10 Publisher Layer

- Interface for downstream delivery of encoded SBE messages
- Contract: `publish(DirectBuffer, offset, length, counters, nanoClock)` — must not block
- Implementations: in-memory (tests), debug/logging, and future adapters
  for Aeron, Chronicle Queue, Kafka, shared memory
- This project does not implement downstream platforms themselves
- Only recognized/configured instruments are published
- Backpressure beyond bounded retries → drop + increment counters + trigger recovery

### 4.11 Recovery Interface Layer

- Venue-agnostic interface for downstream-triggered recovery
- Compatible with Aeron, Kafka, TCP, IPC, in-memory test harnesses
- Carries explicit requests: `venue`, `instrumentId`, `requestType`, `reasonCode`,
  `requestTimestamp`
- Does not carry or reference market-data buffers

### 4.12 Observability

- Hot-path counters use Agrona `CountersManager` and `AtomicCounter`
- Backing store is a memory-mapped buffer in `/dev/shm` (configurable)
- Initialized at startup before any hot-path processing
- Each counter allocated with a descriptive label for generic discovery
- Hot-path ingress timestamp: one Agrona `CachedNanoClock` per connector,
  updated by that connector's event-loop thread once per iteration
- Hot-path errors: Agrona `DistinctErrorLog` (memory-mapped, deduplicated, lock-free)
- Time-dependent components accept `EpochClock` / `NanoClock` abstractions —
  never call `System.currentTimeMillis()` or `System.nanoTime()` directly

**Required counter categories:**

Connection and transport: active WebSocket connections, connection attempts,
successes, failures, reconnects, heartbeats received/missed, last heartbeat
timestamp, last message received timestamp per connection.

Inbound processing: frames received, bytes received, messages parsed by type
(snapshot, update, heartbeat), parse failures, malformed rejections, unknown type drops.

Instrument resolution: unknown symbol drops, snapshots/updates received per instrument.

Encoding: encode successes/failures, overflow rejections, levels encoded,
encoded bytes, encode buffer reuse count.

Publishing: messages published, publish failures, backpressure events and drops.

Recovery: requests received/ignored/executed/failed, BOOK_RESET and BOOK_SNAPSHOT
events published, recovery-in-progress state per instrument.

Sequence tracking: sequence gaps detected, out-of-order messages, last sequence
per connection.

### 4.13 Metrics Export

- HTTP `/metrics` endpoint on a dedicated non-hot-path thread
- Uses Agrona `CountersReader` to iterate counters generically — new counters
  are discoverable without code changes
- Emits Prometheus-compatible text format (version 0.0.4)
- Latency: min, max, last ingress-to-publish duration tracked via counters
- Full percentile tracking (HdrHistogram) deferred until Phase 3 is stable
- Each instance exposes its own endpoint; Prometheus scrapes all instances
  independently; aggregation via Prometheus queries and Grafana dashboards

#### Prometheus Label Schema

Every metric carries these labels:

| Label | Source | Example |
|---|---|---|
| `instance_id` | `GatewayConfig.instanceId` | `"gateway-coinbase-l2-1"` |
| `environment` | `GatewayConfig.environment` | `"prod"` |
| `venue` | `VenueEnum.name()` | `"COINBASE_L2"` |
| `instrument_id` | `InstrumentConfig.instrumentId` | `"1001"` |

Connection-level counters carry only `instance_id`, `environment`, and `venue`.
Per-instrument counters carry all four labels.

#### Metric Name Convention

All metric names are prefixed `gateway_` and suffixed with the Prometheus type
convention (`_total` for counters, `_bytes` for byte sizes, `_nanos` for
nanosecond timestamps/latencies). Names use snake_case.

#### Metric Catalogue

```
# Connection and transport
gateway_active_connections              gauge   — current open WS connections
gateway_connection_attempts_total       counter
gateway_connection_successes_total      counter
gateway_connection_failures_total       counter
gateway_reconnect_attempts_total        counter

# Inbound processing (per instrument)
gateway_frames_received_total           counter
gateway_bytes_received_total            counter
gateway_messages_decoded_total          counter
gateway_snapshot_messages_received_total counter
gateway_update_messages_received_total  counter
gateway_heartbeats_received_total       counter
gateway_parse_failures_total            counter
gateway_malformed_rejections_total      counter
gateway_unknown_type_drops_total        counter
gateway_pre_snapshot_drops_total        counter

# Instrument resolution (per instrument)
gateway_unknown_symbol_drops_total      counter
gateway_product_id_mismatches_total     counter
gateway_subscription_validation_failures_total counter
gateway_authentication_errors_total     counter

# Encoding (per instrument)
gateway_encode_successes_total          counter
gateway_encode_failures_total           counter
gateway_overflow_rejections_total       counter
gateway_levels_encoded_total            counter
gateway_encoded_bytes_total             counter
gateway_encode_buffer_reuse_count       gauge   — equals SbeEncoder.reuseCount()

# Publishing (per instrument)
gateway_messages_published_total        counter
gateway_publish_failures_total          counter
gateway_backpressure_events_total       counter
gateway_backpressure_drops_total        counter

# Recovery (per instrument)
gateway_recovery_attempts_total         counter
gateway_recovery_completions_total      counter
gateway_recovery_failures_total         counter
gateway_recovery_executions_total       counter
gateway_recovery_requests_ignored_total counter
gateway_book_reset_published_total      counter
gateway_book_snapshot_published_total   counter

# Sequence (per instrument)
gateway_sequence_gaps_total             counter
gateway_out_of_order_messages_total     counter
gateway_checksum_failures_total         counter  # Kraken L2 only

# Liveness (per instrument)
gateway_heartbeats_missed_total         counter
gateway_liveness_failures_total         counter
gateway_last_heartbeat_received_nanos   gauge    # epoch nanos
gateway_last_message_received_nanos     gauge    # epoch nanos

# Connector handoff latency (per instrument)
gateway_handoff_latency_min_nanos       gauge
gateway_handoff_latency_max_nanos       gauge
gateway_handoff_latency_last_nanos      gauge

# Publisher latency (required for all publishers)
publisher_latency_last_nanos            gauge
publisher_latency_min_nanos             gauge
publisher_latency_max_nanos             gauge
```

#### Sample Prometheus Output

```
# HELP gateway_messages_published_total Total SBE messages successfully published
# TYPE gateway_messages_published_total counter
gateway_messages_published_total{instance_id="gateway-coinbase-l2-1",environment="prod",venue="COINBASE_L2",instrument_id="1001"} 4823901

# HELP gateway_handoff_latency_last_nanos Most recent ingress-to-handoff latency in nanoseconds
# TYPE gateway_handoff_latency_last_nanos gauge
gateway_handoff_latency_last_nanos{instance_id="gateway-coinbase-l2-1",environment="prod",venue="COINBASE_L2",instrument_id="1001"} 3241

# HELP gateway_recovery_completions_total Recoveries where snapshot boundary was accepted
# TYPE gateway_recovery_completions_total counter
gateway_recovery_completions_total{instance_id="gateway-coinbase-l2-1",environment="prod",venue="COINBASE_L2",instrument_id="1001"} 2
```

#### `MetricsEndpoint` Implementation Contract

```java
// core/observability/MetricsEndpoint.java
/**
 * HTTP server that serves Prometheus-format metrics on a dedicated thread.
 * Uses a simple blocking HTTP server (e.g. com.sun.net.httpserver.HttpServer)
 * — no Netty, no Spring. Runs on a non-hot-path thread.
 *
 * On each GET /metrics request:
 *   1. Acquire a CountersReader snapshot from the CountersManager
 *   2. Iterate all allocated counters
 *   3. For each counter: parse label metadata from the counter's label string,
 *      emit # HELP, # TYPE, and the metric line
 *   4. Flush and return HTTP 200 text/plain; charset=utf-8
 *
 * Label metadata is embedded in the counter label string at allocation time
 * in the format: "metric_name|label_key=label_value,..."
 * The MetricsEndpoint parses this to reconstruct the Prometheus line.
 *
 * Thread safety: CountersReader is read-only; the underlying mapped buffer
 * is written lock-free by the event-loop threads. Reads may observe a slightly
 * stale value for any given counter — this is acceptable for metrics scraping.
 */
public final class MetricsEndpoint {
    public MetricsEndpoint(CountersManager manager, int httpPort) { ... }
    public void start() { ... }   // starts the HTTP server thread
    public void stop()  { ... }   // stops the HTTP server thread
}
```

---

## 5. Core Interfaces

### 5.1 `Connector`

```java
// core/connector/Connector.java
public interface Connector {
    /**
     * Called once by GatewayBootstrap after construction.
     * Unpacks ConnectorContext into fields and calls buildSessionContexts().
     * Must NOT hold ConnectorContext as a field after this method returns.
     */
    void init(ConnectorContext ctx);

    /** Dial, TLS handshake, WebSocket upgrade, authenticate, subscribe. */
    void connect();

    /**
     * Publishes BOOK_RESET then delegates to RecoveryStrategy.
     * Must be called on the owning event-loop thread.
     */
    void recover(RecoveryRequest request);

    /**
     * Sends authenticated unsubscribe, publishes best-effort BOOK_RESET,
     * closes connection, releases resources within the configured deadline.
     */
    void shutdown();

    VenueEnum venue();
    int instrumentId();
}
```

### 5.2 `ConnectorFactory`

```java
// core/connector/ConnectorFactory.java
/**
 * ServiceLoader SPI. One implementation per venue package.
 * Registered in META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory.
 */
public interface ConnectorFactory {
    /** The venue this factory handles. Used by VenueRegistry to match config. */
    VenueEnum venue();

    /**
     * Creates one Connector for the given instrument.
     * Called once per instrument at startup. May allocate freely.
     * Gateway resources arrive later via Connector.init(ConnectorContext).
     */
    Connector create(InstrumentConfig instrument, GatewayConfig config);
}
```

### 5.3 `FeedParser`

```java
// core/parser/FeedParser.java
/**
 * Parses inbound WebSocket frames directly from a Netty off-heap ByteBuf.
 * Implementations must be allocation-free on the hot path.
 * One instance per Connector.
 *
 * DESIGN: ParseContext is a dependency carrier. The parser calls methods on
 * ctx.encoder(), ctx.publisher(), ctx.sequenceTracker(), etc. directly.
 * It does not call encoding methods on ctx itself.
 *
 * IMPLEMENTOR RULES:
 *   - Hold no session state as fields.
 *   - Never hold ParseContext as a field.
 *   - Increment ctx.counters() for every parse outcome.
 *   - Check ctx.snapshotGatekeeper().isReady() before any BOOK_UPDATE.
 */
public interface FeedParser {

    /**
     * Called for UTF-8 text frames. Primary entry point for JSON venues.
     * The frame ByteBuf is released by the caller after this returns.
     * Do not retain it.
     */
    void onTextFrame(ByteBuf frame, ParseContext ctx);

    /**
     * Called for binary WebSocket frames.
     * Default: count as unknown and return.
     * Override for binary-frame venues (e.g. per-message deflate).
     */
    default void onBinaryFrame(ByteBuf frame, ParseContext ctx) {
        ctx.counters().unknownMessageTypeDrops().increment();
    }
}
```

### 5.4 `SubscriptionBuilder`

```java
// core/subscription/SubscriptionBuilder.java
public interface SubscriptionBuilder {
    /** Builds an authenticated subscribe payload. Returns UTF-8 JSON bytes. */
    byte[] buildSubscribe(InstrumentConfig instrument);

    /** Builds an authenticated unsubscribe payload. Returns UTF-8 JSON bytes. */
    byte[] buildUnsubscribe(InstrumentConfig instrument);
}
```

### 5.5 `SnapshotStrategy`

```java
// core/snapshot/SnapshotStrategy.java
public interface SnapshotStrategy {

    enum Mode {
        /** Snapshot arrives automatically on subscribe (e.g. Coinbase L2). */
        SUBSCRIBE_DRIVEN,
        /**
         * Snapshot must be fetched via REST; WS updates buffered and aligned.
         * Uses Java 11 HttpClient on a non-hot-path thread (e.g. Binance L2).
         *
         * REQUIRED ALGORITHM for REST_THEN_DELTA implementations:
         *
         * 1. triggerSnapshot() opens a pre-allocated ring buffer for inbound
         *    WS delta frames. All frames are buffered from this point.
         * 2. Issue async REST snapshot request on a non-hot-path thread.
         * 3. REST snapshot arrives with a sequence boundary (e.g. lastUpdateId N).
         *    Schedule processing back to the connector's event-loop thread.
         * 4. On event-loop thread: encode and publish BOOK_SNAPSHOT.
         * 5. Drain the buffer in arrival order:
         *    a. Drop any delta where finalUpdateId <= N (before snapshot).
         *    b. Find the first delta where firstUpdateId <= N+1 AND
         *       finalUpdateId >= N+1. This covers the snapshot boundary.
         *    c. If no such delta exists: discard all buffered deltas;
         *       wait for the next live delta that qualifies.
         *    d. If a sequence gap appears (delta.firstUpdateId >
         *       prevDelta.finalUpdateId + 1): signal stream integrity failure,
         *       which triggers recovery.
         * 6. Encode and publish each valid delta as BOOK_UPDATE in order.
         * 7. Call snapshotGatekeeper().accept() — opens the gate for live updates.
         * 8. All subsequent live frames pass through the parser normally.
         *
         * The ring buffer must be pre-allocated at construction time. It must
         * not grow on the hot path. If it fills before the REST snapshot arrives,
         * signal stream integrity failure and let recovery restart the process.
         */
        REST_THEN_DELTA
    }

    Mode mode();

    /**
     * Called by the Connector after every subscribe or resubscribe.
     * SUBSCRIBE_DRIVEN: no-op — snapshot arrives via the normal feed.
     * REST_THEN_DELTA: opens the delta buffer and starts the async REST fetch.
     *                  Processing is completed asynchronously per the algorithm above.
     */
    void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx);
}
```

### 5.6 `RecoveryStrategy`

```java
// core/recovery/RecoveryStrategy.java
/**
 * IMPLEMENTOR CONTRACT:
 * Must call ctx.onChannelRestored() OR ctx.onRecoveryFailed() exactly once.
 * Calling neither: connector stuck in recovery-in-progress state permanently.
 * Calling both: protocol violation.
 */
public interface RecoveryStrategy {
    void execute(RecoveryRequest request, RecoveryContext ctx);
}
```

### 5.7 `Publisher`

```java
// core/publisher/Publisher.java
public interface Publisher {

    /**
     * Publishes the encoded message in the given buffer region.
     * Called on the Netty event-loop thread.
     *
     * BUFFER OWNERSHIP CONTRACT — this is the critical rule:
     *
     *   The publisher does NOT own and may NOT retain the buffer reference
     *   after this method returns. The encoder's reusable buffer will be
     *   overwritten on the next beginMessage() call, which may happen
     *   immediately after this method returns.
     *
     *   Synchronous implementations: read or hand off all data before returning.
     *   Asynchronous implementations: copy the bytes [offset, offset+length) into
     *     an independently-owned buffer (e.g. a slot in a pre-allocated off-heap
     *     ring or pool) before returning. The copy must complete before returning.
     *     Storing the DirectBuffer reference for later use is a data corruption bug.
     *
     * Must not block the event-loop thread.
     *
     * @param buffer    the encoder's reusable off-heap buffer — valid only for
     *                  the duration of this call
     * @param offset    start of the encoded message within buffer
     * @param length    byte length of the encoded message
     * @param counters  per-instrument counters for this message; used to update
     *                  publisher-stage latency on the shared publisher
     * @param nanoClock caller-owned clock for publisher-side timing. The exact
     *                  timing strategy is implementation-defined by this spec revision.
     * @return        true if the message was delivered or enqueued;
     *                false if backpressure prevented delivery
     */
    boolean publish(DirectBuffer buffer, int offset, int length,
                    InstrumentCounters counters,
                    NanoClock nanoClock);

    /**
     * Publishes a zero-entry BOOK_RESET event for the given instrument.
     * The same buffer ownership contract applies — the reset event must be
     * consumed or copied before this method returns.
     *
     * REENTRANCY GUARANTEE: publishReset() is only called by AbstractConnector
     * at two clean boundaries where no hot-path encoding is in progress:
     *   1. In recover() — before RecoveryStrategy.execute() is called
     *   2. In shutdown() — before transport.close() is called
     * It is never called mid-frame from any parser or strategy code. Venue
     * implementations must not call publishReset() directly; they use the
     * connector's recover() or the RecoveryContext callbacks instead.
     * @param venueByte immutable venue byte for this connector's instrument
     * @param bookDepthByte immutable depth byte for this connector's instrument
     * @param templateIdByte immutable template byte for this connector's instrument
     * @param nanoClock caller-owned clock used to stamp the reset message's
     *                  ingressTimestamp. This control-path operation must NOT
     *                  update `publisher_latency_*`.
     * Best-effort — must not block.
     */
    void publishReset(int instrumentId, byte venueByte,
                      byte bookDepthByte, byte templateIdByte,
                      NanoClock nanoClock);
}
```

#### Backpressure Retry Contract

When `publish()` returns `false`, the caller (always the event-loop thread)
must apply the following bounded retry policy before declaring backpressure
failure and dropping:

```
maxRetries   = 3            // total publish attempts (1 initial + 2 retries)
retryStrategy = Thread.onSpinWait()  // bounded CPU hint between retries;
                                     // stays on-core and avoids scheduler handoff

for attempt in [1..maxRetries]:
    if publisher.publish(buffer, offset, length, counters, nanoClock):
        return SUCCESS
    if attempt < maxRetries:
        Thread.onSpinWait()

// All retries exhausted:
counters.backpressureDrops().increment()
counters.backpressureEvents().increment()
triggerRecovery(RESET, STREAM_INTEGRITY_FAILURE)
return DROPPED
```

`maxRetries` and the retry hint strategy are not configurable — they are fixed in
`AbstractConnector`. Making them configurable would introduce branches on the
hot path. If a publisher consistently cannot accept within 3 attempts, the root
cause is downstream capacity, not a tuning parameter.

`Thread.onSpinWait()` is chosen over `Thread.yield()` / `LockSupport.parkNanos()` because:
- The event-loop thread must not sleep — it would miss inbound frames
- `yield()` is scheduler-dependent and may not improve latency portability across JVM/OS combinations
- `parkNanos()` introduces a real park/unpark boundary, which is too heavy for a 3-attempt failure-path retry
- A tiny bounded spin is acceptable at this retry count and should be verified with benchmark data on the deployment platform

#### In-Memory Test Publisher

All Phase 1 and Phase 2 tests use `InMemoryPublisher`, the sole Publisher
implementation provided by `core/`. It captures encoded messages in a
pre-allocated array for assertion in tests.

```java
// core/publisher/InMemoryPublisher.java
/**
 * Synchronous, always-accepting Publisher for use in tests.
 * Copies each published message into a slot in a fixed-size ring buffer.
 * Never returns false from publish() — simulates an always-available downstream.
 *
 * Thread-compatible (not thread-safe): designed for single-event-loop-thread use,
 * matching the production hot-path contract.
 *
 * NOT suitable for production use — no real downstream delivery.
 */
public final class InMemoryPublisher implements Publisher {

    private static final int DEFAULT_CAPACITY = 1024; // message slots

    private final byte[][] slots;      // pre-allocated byte arrays, one per slot
    private final int[]    lengths;    // byte length of each captured message
    private int            writeIdx;   // next slot to write into (wraps)
    private int            totalCount; // total messages published (does not wrap)

    public InMemoryPublisher() { this(DEFAULT_CAPACITY, 300_000); } // 300 KB max per message

    public InMemoryPublisher(int capacity, int maxMessageBytes) {
        this.slots   = new byte[capacity][maxMessageBytes];
        this.lengths = new int[capacity];
    }

    @Override
    public boolean publish(DirectBuffer buffer, int offset, int length,
                           InstrumentCounters counters,
                           NanoClock nanoClock) {
        int slot = writeIdx % slots.length;
        buffer.getBytes(offset, slots[slot], 0, length);  // copy into pre-allocated array
        lengths[slot] = length;
        writeIdx++;
        totalCount++;
        // Timing strategy intentionally deferred. nanoClock is passed so publisher-side
        // latency can be implemented later without changing the publish() signature.
        return true;
    }

    @Override
    public void publishReset(int instrumentId, byte venueByte,
                             byte bookDepthByte, byte templateIdByte,
                             NanoClock nanoClock) {
        // encode and capture a BOOK_RESET message using venueByte,
        // bookDepthByte, templateIdByte, and nanoClock.nanoTime(); implementation omitted for brevity
    }

    // When publisher-stage timing is enabled, measure latency using the chosen
    // NanoClock strategy and then:
    // PublisherLatencyStats.record(counters, latencyNanos);

    /** Returns the most recently published raw message bytes. */
    public byte[] lastMessage() {
        int slot = (writeIdx - 1 + slots.length) % slots.length;
        return Arrays.copyOf(slots[slot], lengths[slot]);
    }

    /** Returns all captured messages in publication order (oldest first if wrapped). */
    public List<byte[]> allMessages() { ... }

    /** Total number of publish() calls since construction. */
    public int totalCount() { return totalCount; }

    /** Resets the publisher state between test cases. */
    public void reset() { writeIdx = 0; totalCount = 0; }
}
```

Tests access published bytes directly via `lastMessage()` or `allMessages()`
and decode them using an `SbeDecoder` (test utility, not a production class)
to assert field values byte-by-byte.

### 5.8 `PipelineCustomizer`

```java
// core/transport/PipelineCustomizer.java
@FunctionalInterface
public interface PipelineCustomizer {
    /**
     * Called during Netty pipeline setup.
     * Venue adds handlers here (e.g. WebSocketClientCompressionHandler).
     * Most venues pass: pipeline -> {}
     */
    void customize(ChannelPipeline pipeline);
}
```

---

## 6. Core Types

### 6.1 `SbeEncoder`

```java
// core/encoding/SbeEncoder.java
/**
 * Encodes market data into SBE-style binary format into a reusable off-heap
 * Agrona MutableDirectBuffer. One instance per Connector.
 *
 * instrumentId, venue, bookDepth, and templateId are set at construction —
 * the encoder always encodes the same instrument, venue, depth level, and
 * template. These are not per-message parameters.
 *
 * NOT thread-safe. Owned exclusively by one Connector / event-loop thread.
 * The encode buffer is allocated once at construction. No per-message allocation.
 *
 * Use writeLevel() for BOOK_LEVEL template (L1/L2 strategies).
 * Use writeOrder() for ORDER_ENTRY template (L3 strategies).
 * Calling the wrong write method for the configured template is a contract violation.
 */
public final class SbeEncoder {

    /**
     * @param instrumentId  stable configured internal ID for this instrument
     * @param venueByte     VenueEnum byte value (e.g. COINBASE_L2=1)
     * @param bookDepthByte BookDepth byte value (L1=1, L2=2, L3=3)
     * @param templateIdByte TemplateId byte value (BOOK_LEVEL=1, ORDER_ENTRY=2)
     * @param maxEntryCount maximum repeating-group entries per message (10,000)
     * @param headroomBytes additional buffer capacity beyond max encoded size
     */
    public SbeEncoder(int instrumentId, byte venueByte,
                      byte bookDepthByte, byte templateIdByte,
                      int maxEntryCount, int headroomBytes) { ... }

    /**
     * Begins encoding a new message. Must precede all write calls.
     *
     * REENTRANCY INVARIANT: beginMessage() must only be called when no message
     * is currently in progress (i.e. after the previous endMessage() returned,
     * or at the start of a session). Calling beginMessage() while a previous
     * message is partially written overwrites the encode buffer — undefined behavior.
     *
     * AbstractConnector guarantees this invariant: publishReset() is only called
     * at two defined clean boundaries — before recovery.execute() in recover(),
     * and before transport.close() in shutdown() — never during a hot-path frame.
     *
     * entryCount is NOT a parameter — it is tracked internally and written
     * by endMessage() after all writeLevel()/writeOrder() calls complete.
     * This enables true single-pass encoding: the parser loops over entries
     * without needing to pre-count them.
     *
     * @param eventType         BOOK_RESET=1, BOOK_SNAPSHOT=2, BOOK_UPDATE=3
     * @param gatewayMessageSeq per-session monotonic counter from SequenceTracker.next()
     * @param seq1              start sequence — gateway-managed or exchange-native
     * @param seq2              end sequence  — gateway-managed or exchange-native
     * @param exchangeTimestamp epoch nanoseconds, or -1 if absent
     * @param ingressTimestamp  epoch nanoseconds from CachedNanoClock
     */
    public void beginMessage(byte eventType,
                             long gatewayMessageSeq,
                             long seq1, long seq2,
                             long exchangeTimestamp, long ingressTimestamp) {
        // writes header + body; sets entryCount placeholder = 0 at ENTRY_COUNT_OFFSET
        // resets internal entryCount to 0
    }

    /**
     * Writes one aggregated price level into the repeating group.
     * Use for BOOK_LEVEL template (L1 and L2 strategies) only.
     * Call any number of times after beginMessage(). Increments internal entryCount.
     */
    public void writeLevel(byte side, byte action,
                           long priceMantissa, byte priceScale,
                           long qtyMantissa,   byte qtyScale) {
        // writes 20 bytes at current position; entryCount++
    }

    /**
     * Writes one individual order entry into the repeating group.
     * Use for ORDER_ENTRY template (L3 strategies) only.
     * Call any number of times after beginMessage(). Increments internal entryCount.
     *
     * @param orderId    stable exchange-assigned order identifier
     * @param side       BID=1, ASK=2
     * @param action     UPSERT=1 (new or modified order), DELETE=2 (cancelled/filled)
     * @param orderType  LIMIT=1, MARKET=2, STOP=3
     */
    public void writeOrder(long orderId,
                           byte side, byte action, byte orderType,
                           long priceMantissa, byte priceScale,
                           long qtyMantissa,   byte qtyScale) {
        // writes 29 bytes at current position; entryCount++
    }

    /**
     * Finalizes the encoded message and hands it to the publisher.
     *
     * Patches ENTRY_COUNT_OFFSET in the header with the actual count of
     * writeLevel()/writeOrder() calls made since beginMessage(). This is
     * the mechanism that enables single-pass encoding — entryCount is
     * written last, after all entries are known.
     *
     *   buffer.putShort(ENTRY_COUNT_OFFSET, (short) entryCount, LITTLE_ENDIAN);
     *
     * BUFFER RECLAIM RULE: the encoder's reusable buffer is reclaimed for
     * the next message as soon as this method returns. The publisher must
     * not retain the DirectBuffer reference beyond its publish() call.
     *
     * If publish() returns false (backpressure), the message is lost.
     * The caller is responsible for incrementing backpressure counters and
     * triggering recovery when appropriate.
     *
     * Increments reuseCount on every call regardless of publish outcome.
     *
     * @return true if published successfully; false on backpressure
     */
    public boolean endMessage(Publisher publisher, InstrumentCounters counters,
                              NanoClock nanoClock) {
        // patch entryCount at ENTRY_COUNT_OFFSET, then publish
    }

    /** Total messages encoded — equals encode buffer reuse count. */
    public long reuseCount() { return reuseCount; }

    /** Read-only buffer access for tests only. */
    public DirectBuffer buffer() { return buffer; }
}
```

### 6.2 `SequenceTracker`

```java
// core/sequence/SequenceTracker.java
/**
 * Tracks gateway-managed outbound sequence numbers for one connector session.
 * Starts at 0. next() returns 1 on the first call. reset() returns to 0.
 *
 * Sequence mapping by venue:
 *
 *   Coinbase (no exchange sequence in level2):
 *     long seq = ctx.sequenceTracker().next();
 *     encoder.beginMessage(eventType, seq, seq, seq, exchangeTs, ingressTs);
 *     // gatewayMessageSeq = seq1 = seq2 = same gateway counter
 *
 *   Binance (exchange-native range sequence U/u):
 *     long gatewaySeq = ctx.sequenceTracker().next();
 *     long seq1 = parseFirstUpdateId(frame);  // U field
 *     long seq2 = parseFinalUpdateId(frame);  // u field
 *     encoder.beginMessage(eventType, gatewaySeq, seq1, seq2, exchangeTs, ingressTs);
 *
 *   Kraken (no exchange sequence — checksum-based integrity):
 *     long seq = ctx.sequenceTracker().next();
 *     encoder.beginMessage(eventType, seq, seq, seq, exchangeTs, ingressTs);
 *     // checksum validated inside parser; mismatch triggers recovery
 *
 * Reset via reset() at the start of every recovery
 * (called automatically by DefaultParseContext.resetSession()).
 */
public final class SequenceTracker {

    private long seq = 0;

    /** Increments and returns. Starts at 1 after construction or reset(). */
    public long next()    { return ++seq; }

    /** Current value without incrementing. */
    public long current() { return seq; }

    /** Resets to 0. Next next() call returns 1. */
    public void reset()   { seq = 0; }
}
```

### 6.3 `SnapshotGatekeeper`

```java
// core/snapshot/SnapshotGatekeeper.java
/**
 * Gates BOOK_UPDATE publication until the first valid snapshot boundary
 * is accepted for this session.
 *
 * The same instance is shared between ParseContext and SnapshotContext.
 * When the snapshot strategy calls accept(), the parser's gate opens
 * immediately on the next frame — no additional signaling needed.
 *
 * Reset via reset() at the start of every recovery
 * (called automatically by DefaultParseContext.resetSession()).
 */
public final class SnapshotGatekeeper {

    private boolean ready = false;

    /**
     * Returns true only after accept() has been called this session.
     * FeedParser checks this before publishing any BOOK_UPDATE.
     * If false, the frame must be dropped and counted as preSnapshotDrop.
     */
    public boolean isReady() { return ready; }

    /**
     * Called by SnapshotStrategy when the first valid snapshot boundary
     * is accepted. Opens the BOOK_UPDATE gate. Call exactly once per session.
     */
    public void accept() { ready = true; }

    /** Resets to WAITING. Called by DefaultParseContext.resetSession(). */
    public void reset()  { ready = false; }
}
```

### 6.4 `BookDepth`

```java
// core/config/BookDepth.java
/**
 * The depth level of market data a venue strategy produces.
 * Encoded as a fixed byte in every SBE message body so downstream
 * consumers know which repeating-group decoder to apply without
 * inspecting the venue byte alone.
 *
 * L1 — top of book only (best bid / best ask)
 * L2 — aggregated price levels (total quantity per price level)
 * L3 — individual orders (one entry per resting order)
 *
 * Set once at SbeEncoder construction time by the ConnectorFactory.
 * Never changes per message. The byte value is part of the wire
 * format and is immutable once assigned.
 */
public enum BookDepth {
    L1 ((byte) 1),
    L2 ((byte) 2),
    L3 ((byte) 3);

    private final byte byteValue;
    BookDepth(byte byteValue) { this.byteValue = byteValue; }
    public byte byteValue() { return byteValue; }
}
```

### 6.5 `TemplateId`

```java
// core/config/TemplateId.java
/**
 * Identifies the SBE message template — i.e. the exact binary layout
 * of the repeating group. Written into the SBE header templateId field.
 *
 * BOOK_LEVEL  — repeating group contains aggregated price levels (L1 or L2).
 *               Each entry: side, action, priceScale, qtyScale,
 *                           priceMantissa, qtyMantissa.
 *
 * ORDER_ENTRY — repeating group contains individual orders (L3).
 *               Each entry: orderId, side, action, orderType,
 *                           priceScale, qtyScale, priceMantissa, qtyMantissa.
 *
 * Set once at SbeEncoder construction time by the ConnectorFactory.
 * Never changes per message. The byte value is part of the wire
 * format and is immutable once assigned.
 */
public enum TemplateId {
    BOOK_LEVEL  ((byte) 1),
    ORDER_ENTRY ((byte) 2);

    private final byte byteValue;
    TemplateId(byte byteValue) { this.byteValue = byteValue; }
    public byte byteValue() { return byteValue; }
}
```

### 6.6 `GatewayCounters`

```java
// core/observability/GatewayCounters.java
/**
 * Facade over Agrona CountersManager that exposes all required counters
 * as named methods. Each counter is backed by an Agrona AtomicCounter —
 * a cache-line-padded off-heap long accessible via Unsafe CAS.
 *
 * GatewayCounters is allocated once per gateway instance and shared across
 * all connectors. Per-instrument counters are obtained via forInstrument().
 *
 * All counters are pre-allocated at startup — no counter is created on the
 * hot path. Counter labels include instanceId, venue, and instrumentId where
 * applicable, for Prometheus label export.
 */
public final class GatewayCounters {

    public GatewayCounters(CountersManager manager, String instanceId,
                            String environment, VenueEnum venue) { ... }

    /**
     * Returns a per-instrument counter set. Called once per connector in init().
     * The returned object holds references to pre-allocated AtomicCounters
     * scoped to this instrumentId. Not cached — callers cache the result.
     */
    public InstrumentCounters forInstrument(InstrumentConfig instrument) { ... }

    // ── Connection and transport ───────────────────────────────────────────
    public AtomicCounter activeConnections()     { ... } // gauge: increment on connect, decrement on close
    public AtomicCounter connectionAttempts()    { ... } // total dial attempts
    public AtomicCounter connectionSuccesses()   { ... } // successful upgrades
    public AtomicCounter connectionFailures()    { ... } // failed dials
    public AtomicCounter reconnectAttempts()     { ... } // reconnect loop iterations
}
```

```java
// core/observability/InstrumentCounters.java
/**
 * Per-instrument counters. One instance per Connector, created in init().
 * All methods return pre-allocated AtomicCounters — no allocation on call.
 */
public final class InstrumentCounters {

    // ── Inbound processing ────────────────────────────────────────────────
    public AtomicCounter framesReceived()               { ... }
    public AtomicCounter bytesReceived()                { ... }
    public AtomicCounter messagesDecoded()              { ... }
    public AtomicCounter snapshotMessagesReceived()     { ... }
    public AtomicCounter updateMessagesReceived()       { ... }
    public AtomicCounter heartbeatsReceived()           { ... }
    public AtomicCounter parseFailures()                { ... }
    public AtomicCounter malformedRejections()          { ... }
    public AtomicCounter unknownTypeDrops()             { ... }
    public AtomicCounter preSnapshotDrops()             { ... }  // updates before first snapshot

    // ── Instrument resolution ─────────────────────────────────────────────
    public AtomicCounter unknownSymbolDrops()           { ... }
    public AtomicCounter productIdMismatches()          { ... }

    // ── Encoding ──────────────────────────────────────────────────────────
    public AtomicCounter encodeSuccesses()              { ... }
    public AtomicCounter encodeFailures()               { ... }
    public AtomicCounter overflowRejections()           { ... }  // > 10,000 entries
    public AtomicCounter levelsEncoded()                { ... }  // repeating-group entries total
    public AtomicCounter encodedBytes()                 { ... }
    public AtomicCounter encodeBufferReuseCount()       { ... }  // = SbeEncoder.reuseCount()

    // ── Publishing ────────────────────────────────────────────────────────
    public AtomicCounter messagesPublished()            { ... }
    public AtomicCounter publishFailures()              { ... }
    public AtomicCounter backpressureEvents()           { ... }  // publish returned false
    public AtomicCounter backpressureDrops()            { ... }  // dropped after retry exhaustion

    // ── Recovery ──────────────────────────────────────────────────────────
    public AtomicCounter recoveryAttempts()             { ... }  // recover() called
    public AtomicCounter recoveryCompletions()          { ... }  // Phase B: snapshot accepted
    public AtomicCounter recoveryFailures()             { ... }  // onRecoveryFailed() called
    public AtomicCounter recoveryExecutions()           { ... }  // strategy.execute() called
    public AtomicCounter recoveryRequestsIgnoredInProgress() { ... }
    public AtomicCounter bookResetPublished()           { ... }
    public AtomicCounter bookSnapshotPublished()        { ... }
    public AtomicCounter subscriptionValidationFailures() { ... }
    public AtomicCounter authenticationErrors()         { ... }  // inbound error frame received

    // ── Sequence tracking ─────────────────────────────────────────────────
    public AtomicCounter sequenceGapsDetected()         { ... }
    public AtomicCounter outOfOrderMessages()           { ... }
    public AtomicCounter checksumFailures()             { ... }  // Kraken L2 only

    // ── Liveness ──────────────────────────────────────────────────────────
    public AtomicCounter heartbeatsMissed()             { ... }  // timeout firings
    public AtomicCounter livenessFailures()             { ... }  // timeout-triggered recovery

    // ── Connector handoff latency (updated before publisher.publish()) ────
    public AtomicCounter handoffLatencyMinNanos()       { ... }
    public AtomicCounter handoffLatencyMaxNanos()       { ... }
    public AtomicCounter handoffLatencyLastNanos()      { ... }

    // ── Publisher-stage latency (updated by Publisher observability) ───────
    public AtomicCounter publisherLatencyMinNanos()     { ... }
    public AtomicCounter publisherLatencyMaxNanos()     { ... }
    public AtomicCounter publisherLatencyLastNanos()    { ... }

    // ── Heartbeat liveness ────────────────────────────────────────────────
    public AtomicCounter lastHeartbeatReceivedNanos()   { ... }  // epoch nanos of last heartbeat
    public AtomicCounter lastMessageReceivedNanos()     { ... }  // epoch nanos of last frame
}
```

---

## 7. Context Object Design Pattern

### 7.1 Purpose and Core Rule

The gateway uses four context interfaces to carry dependencies across component
boundaries without per-frame parameter explosion and without per-frame allocation.

> **The Connector is the single owner of all context objects.**
> It creates, holds, resets, and rebuilds them.
> Venue implementations (FeedParser, SnapshotStrategy, RecoveryStrategy)
> are stateless. They receive a context, do their work, and return.
> They must never hold a context object as a field.

**ParseContext is a dependency carrier, not a facade.**
The parser calls methods on the objects returned by the context
(`ctx.encoder().beginMessage(...)`, `ctx.publisher()`, `ctx.nanoClock().nanoTime()`,
etc.) directly. ParseContext does not proxy or wrap encoding calls.

| Interface | Created by | Passed to | Scope | On recovery |
|---|---|---|---|---|
| `ConnectorContext` | `GatewayBootstrap` | `Connector.init()` | One per connector | Never touched |
| `ParseContext` | `Connector.init()` | `FeedParser.onTextFrame()` | Connector lifetime | `resetSession()` called — same object, session state renewed in place |
| `SnapshotContext` | `Connector` | `SnapshotStrategy` methods | Session lifetime | Rebuilt — must point at the new `SnapshotGatekeeper` from `resetSession()` |
| `RecoveryContext` | `Connector` | `RecoveryStrategy.execute()` | Per recovery attempt | Rebuilt — models a single attempt |

### 7.2 `ConnectorContext`

```java
// core/connector/ConnectorContext.java
/**
 * Carries shared gateway resources plus connector-local execution resources
 * from GatewayBootstrap into each Connector.
 * Passed into Connector.init(). Must NOT be held as a field after init() returns.
 */
public interface ConnectorContext {
    GatewayCounters            counters();         // shared Agrona CountersManager-backed counters
    Publisher                  publisher();         // shared downstream publisher
    NanoClock                  nanoClock();         // per-connector CachedNanoClock — updated on that connector's event loop
    EpochClock                 epochClock();        // for control-path timestamps
    TransportConfig            transportConfig();   // heartbeatTimeoutMs, frameSizeLimitBytes, etc.
    EventLoopGroup             eventLoopGroup();    // dedicated group for this connector only
    Map<String, Object>        venueConfig();       // raw venue-specific YAML block for this instance
}
```

### 7.3 `ParseContext`

```java
// core/parser/ParseContext.java
/**
 * Carries session-scoped state and stable encoding resources into FeedParser
 * on every inbound frame.
 *
 * DESIGN: Dependency carrier only. The parser calls through this context to
 * reach encoder(), publisher(), sequenceTracker(), etc. and calls methods on
 * those objects directly. ParseContext exposes no beginMessage(), writeLevel(),
 * endMessage(), or setExchangeTimestamp() methods.
 *
 * LIFECYCLE: Created once in Connector.init() via buildSessionContexts(). Never
 * replaced. On every recovery, the Connector calls parseCtx.resetSession() to
 * renew the two session-scoped fields (SequenceTracker, SnapshotGatekeeper) inside
 * the existing object. The ParseContext object reference held by the Connector
 * does not change across recoveries.
 *
 * HOT PATH: Passed on every frame. Implementations must not allocate.
 * The same DefaultParseContext instance is passed for the entire connector lifetime.
 */
public interface ParseContext {

    // Session-scoped (reset on every recovery)
    SequenceTracker    sequenceTracker();    // reset to 0 on recovery
    SnapshotGatekeeper snapshotGatekeeper(); // reset to WAITING on recovery

    // Fixed for connector lifetime
    SbeEncoder         encoder();    // drives all encoding
    Publisher          publisher();  // passed to encoder().endMessage(...)
    NanoClock          nanoClock();  // for ingressTimestamp capture
    InstrumentCounters counters();   // per-instrument Agrona counters
}
```

### 7.4 `SnapshotContext`

```java
// core/snapshot/SnapshotContext.java
/**
 * Carries what SnapshotStrategy needs to accept, encode, and publish a snapshot.
 *
 * KEY INVARIANT: snapshotGatekeeper() is the SAME instance as in ParseContext.
 * Calling accept() here opens the parser's BOOK_UPDATE gate immediately.
 *
 * After publishing BOOK_SNAPSHOT and calling snapshotGatekeeper().accept(),
 * the snapshot strategy MUST call onSnapshotBoundaryAccepted(). This is the
 * signal that clears the connector's recovery-in-progress flag and increments
 * the recoveryCompletions counter. Failing to call it leaves the flag set
 * permanently, blocking all future recovery requests for this instrument.
 */
public interface SnapshotContext {
    SnapshotGatekeeper snapshotGatekeeper(); // SAME instance as ParseContext
    SbeEncoder         encoder();
    Publisher          publisher();
    InstrumentCounters counters();
    NanoClock          nanoClock();

    /**
     * Called by the snapshot strategy immediately after:
     *   1. The BOOK_SNAPSHOT has been encoded and published, AND
     *   2. snapshotGatekeeper().accept() has been called.
     *
     * This clears the connector's recovery-in-progress flag and increments
     * the recoveryCompletions counter. Must be called exactly once per
     * snapshot boundary establishment (initial connect and each recovery).
     */
    void onSnapshotBoundaryAccepted();
}
```

### 7.5 `RecoveryContext`

```java
// core/recovery/RecoveryContext.java
/**
 * Carries completion callbacks into RecoveryStrategy.execute().
 *
 * RecoveryStrategy knows when the channel work is done (disconnect, reconnect,
 * resubscribe). It does NOT know when the fresh snapshot arrives — that is the
 * responsibility of the SnapshotStrategy, signaled via SnapshotContext.
 *
 * The two recovery events are distinct and signaled separately:
 *   onChannelRestored()             — channel work done; snapshot acquisition started
 *   SnapshotContext.onSnapshotBoundaryAccepted() — snapshot published; flag cleared
 *
 * IMPLEMENTOR CONTRACT (RecoveryStrategy):
 *   Must call onChannelRestored() OR onRecoveryFailed() exactly once per execute().
 *   Do NOT call both. Do NOT call neither.
 */
public interface RecoveryContext {

    /**
     * Signals that channel work is complete: the connection is re-established
     * and the subscribe message has been sent. Snapshot acquisition is now
     * in progress but has NOT yet completed.
     *
     * The connector will reset session state, rebuild context objects, and
     * call SnapshotStrategy.triggerSnapshot(). The recovery-in-progress flag
     * remains SET after this call — it only clears when
     * SnapshotContext.onSnapshotBoundaryAccepted() fires.
     *
     * Must be called exactly once on the success path.
     */
    void onChannelRestored();

    /**
     * Signals that channel work failed (connection error, auth failure, timeout).
     * The connector will increment failure counters and schedule a retry.
     * Must be called exactly once on the failure path.
     */
    void onRecoveryFailed(String reason);

    /** Per-instrument counters for recovery events. */
    InstrumentCounters counters();
}
```

### 7.6 Default Implementations

```java
// core/connector/DefaultConnectorContext.java
public final class DefaultConnectorContext implements ConnectorContext {
    private final GatewayCounters         counters;
    private final Publisher               publisher;
    private final NanoClock               nanoClock;
    private final EpochClock              epochClock;
    private final TransportConfig         transportConfig;
    private final EventLoopGroup          eventLoopGroup;
    private final Map<String, Object>     venueConfig;

    public DefaultConnectorContext(GatewayCounters counters, Publisher publisher,
                                   NanoClock nanoClock, EpochClock epochClock,
                                   TransportConfig transportConfig,
                                   EventLoopGroup eventLoopGroup,
                                   Map<String, Object> venueConfig) {
        this.counters        = counters;
        this.publisher       = publisher;
        this.nanoClock       = nanoClock;
        this.epochClock      = epochClock;
        this.transportConfig = transportConfig;
        this.eventLoopGroup  = eventLoopGroup;
        this.venueConfig     = venueConfig;
    }
    @Override public GatewayCounters         counters()         { return counters;        }
    @Override public Publisher               publisher()        { return publisher;       }
    @Override public NanoClock               nanoClock()        { return nanoClock;       }
    @Override public EpochClock              epochClock()       { return epochClock;      }
    @Override public TransportConfig         transportConfig()  { return transportConfig; }
    @Override public EventLoopGroup          eventLoopGroup()   { return eventLoopGroup;  }
    @Override public Map<String, Object>     venueConfig()      { return venueConfig;     }
}
```

```java
// core/parser/DefaultParseContext.java
public final class DefaultParseContext implements ParseContext {

    // Fixed for connector lifetime
    private final SbeEncoder      encoder;
    private final Publisher       publisher;
    private final InstrumentCounters counters;
    private final NanoClock       nanoClock;

    // Session-scoped — ONLY these two fields change on recovery
    private SequenceTracker    sequenceTracker;
    private SnapshotGatekeeper snapshotGatekeeper;

    public DefaultParseContext(SbeEncoder encoder, Publisher publisher,
                               InstrumentCounters counters, NanoClock nanoClock) {
        this.encoder = encoder; this.publisher = publisher;
        this.counters = counters; this.nanoClock = nanoClock;
        resetSession();
    }

    /**
     * Renews session-scoped state. Called by Connector before every resubscribe.
     * Allocation on the recovery path only — never on the hot path.
     */
    public void resetSession() {
        this.sequenceTracker    = new SequenceTracker();
        this.snapshotGatekeeper = new SnapshotGatekeeper();
    }

    /** Returns the current gatekeeper for sharing with DefaultSnapshotContext. */
    public SnapshotGatekeeper currentSnapshotGatekeeper() { return snapshotGatekeeper; }

    @Override public SequenceTracker    sequenceTracker()    { return sequenceTracker;    }
    @Override public SnapshotGatekeeper snapshotGatekeeper() { return snapshotGatekeeper; }
    @Override public SbeEncoder         encoder()            { return encoder;            }
    @Override public Publisher          publisher()          { return publisher;          }
    @Override public NanoClock          nanoClock()          { return nanoClock;          }
    @Override public InstrumentCounters counters()           { return counters;           }
}
```

```java
// core/snapshot/DefaultSnapshotContext.java
public final class DefaultSnapshotContext implements SnapshotContext {
    private final SnapshotGatekeeper snapshotGatekeeper;
    private final SbeEncoder         encoder;
    private final Publisher          publisher;
    private final InstrumentCounters counters;
    private final NanoClock          nanoClock;
    private final Runnable           onSnapshotAccepted; // fires when boundary is established

    public DefaultSnapshotContext(SnapshotGatekeeper gatekeeper, SbeEncoder encoder,
                                  Publisher publisher, InstrumentCounters counters,
                                  NanoClock nanoClock,
                                  Runnable onSnapshotAccepted) {
        this.snapshotGatekeeper = gatekeeper;
        this.encoder = encoder; this.publisher = publisher; this.counters = counters;
        this.nanoClock = nanoClock;
        this.onSnapshotAccepted = onSnapshotAccepted;
    }
    @Override public SnapshotGatekeeper snapshotGatekeeper() { return snapshotGatekeeper; }
    @Override public SbeEncoder         encoder()            { return encoder;            }
    @Override public Publisher          publisher()          { return publisher;          }
    @Override public InstrumentCounters counters()           { return counters;           }
    @Override public NanoClock          nanoClock()          { return nanoClock;          }

    /**
     * Called by the snapshot strategy after publishing BOOK_SNAPSHOT and
     * calling snapshotGatekeeper().accept(). Routes to the connector's
     * onSnapshotAccepted() callback, which clears the recovery-in-progress
     * flag and increments the recoveryCompletions counter.
     */
    @Override public void onSnapshotBoundaryAccepted() { onSnapshotAccepted.run(); }
}
```

```java
// core/recovery/DefaultRecoveryContext.java
public final class DefaultRecoveryContext implements RecoveryContext {
    private final Runnable         onChannelRestored;
    private final Consumer<String> onFailed;
    private final InstrumentCounters  counters;

    public DefaultRecoveryContext(Runnable onChannelRestored, Consumer<String> onFailed,
                                  InstrumentCounters counters) {
        this.onChannelRestored = onChannelRestored;
        this.onFailed = onFailed; this.counters = counters;
    }
    @Override public void            onChannelRestored()        { onChannelRestored.run(); }
    @Override public void            onRecoveryFailed(String r) { onFailed.accept(r);      }
    @Override public InstrumentCounters counters()              { return counters;         }
}
```

### 7.7 Connector Ownership Pattern

Every venue connector extends `AbstractConnector` from Section 8.1, which owns
all lifecycle, context management, `recoveryInProgress` flag, and both recovery
phase callbacks. A venue connector provides only three declarations:

```java
// venue/<venue>/l<n>/XLnConnector.java
public final class XLnConnector extends AbstractConnector {

    public XLnConnector(InstrumentConfig instrument,
                        XLnFeedParser parser,
                        XLnSnapshotStrategy snapshot,
                        XLnRecoveryStrategy recovery,
                        XLnSubscriptionBuilder subscription) {
        super(instrument, parser, snapshot, recovery, subscription);
    }

    @Override protected VenueEnum venueEnum()     { return VenueEnum.X_LN;  }
    @Override protected int       maxEntryCount() { return 10_000;           }
    @Override protected int       headroomBytes() { return 8_192;            }
}
```

That is the complete connector class. All context wiring, session reset,
`recoveryInProgress` flag management, `onChannelRestored()` callback,
`onSnapshotAccepted()` callback, and counter increments live in `AbstractConnector`.
See Section 8.1 for the full base class code.

### 7.8 Venue Implementation Rules

**Rule 1 — ParseContext is a carrier. Call through it.**

```java
// CORRECT
public void onTextFrame(ByteBuf frame, ParseContext ctx) {
    if (!ctx.snapshotGatekeeper().isReady()) {
        ctx.counters().preSnapshotDrops().increment();
        return;
    }
    long exchangeTs = parseTimestamp(frame);
    long ingressTs  = ctx.nanoClock().nanoTime();
    long seq        = ctx.sequenceTracker().next();
    byte eventType  = parseEventType(frame);

    // No pre-count needed — entryCount is tracked internally by SbeEncoder
    // and patched into the header by endMessage(). True single-pass encoding.
    ctx.encoder().beginMessage(eventType, seq, seq, seq, exchangeTs, ingressTs);
    while (hasNextLevel(frame)) {
        ctx.encoder().writeLevel(parseSide(frame), parseAction(frame),
            parsePriceMantissa(frame), parsePriceScale(frame),
            parseQtyMantissa(frame),   parseQtyScale(frame));
    }
    if (!ctx.encoder().endMessage(ctx.publisher(), ctx.counters(), ctx.nanoClock())) {
        // Apply the bounded backpressure policy from Section 5.6 and return.
        return;
    }
    ctx.counters().messagesDecoded().increment();
}

// WRONG — ParseContext is not a facade
public void onTextFrame(ByteBuf frame, ParseContext ctx) {
    ctx.setExchangeTimestamp(...);   // does not exist
    ctx.beginMessage(...);           // does not exist
    ctx.endMessage();                // does not exist
}
```

**Rule 2 — Stateless. No session state as fields.**

```java
// CORRECT — no fields
public class CoinbaseL2FeedParser implements FeedParser { }

// WRONG
public class CoinbaseL2FeedParser implements FeedParser {
    private SequenceTracker tracker;  // must not be here
}
```

**Rule 3 — Never hold context as a field.**

```java
// WRONG
public class CoinbaseL2FeedParser implements FeedParser {
    private ParseContext ctx; // must not be here
}
```

**Rule 4 — Count every outcome.**

**Rule 5 — Sequence mapping is the parser's responsibility.**

```java
// Coinbase L2 — gateway-managed (no exchange seq in level2 payload)
long seq = ctx.sequenceTracker().next();
ctx.encoder().beginMessage(eventType, seq, seq, seq, exchangeTs, ingressTs);

// Binance L2 — exchange-native range sequence
long gSeq = ctx.sequenceTracker().next();
long seq1 = parseU(frame);
long seq2 = parseLowercaseU(frame);
ctx.encoder().beginMessage(eventType, gSeq, seq1, seq2, exchangeTs, ingressTs);

// Kraken L2 — gateway-managed + checksum validation
if (!validateChecksum(frame)) {
    ctx.counters().checksumFailures().increment();
    // signal stream integrity failure → connector triggers recovery
    return;
}
long seq = ctx.sequenceTracker().next();
ctx.encoder().beginMessage(eventType, seq, seq, seq, exchangeTs, ingressTs);
```

**Rule 6 — SnapshotStrategy: three calls in order after publishing BOOK_SNAPSHOT.**

```java
public void onSnapshotReceived(ByteBuf frame, SnapshotContext ctx) {
    // encode snapshot levels via encoder...
    if (!ctx.encoder().endMessage(ctx.publisher(), ctx.counters(), ctx.nanoClock())) {
        // Apply the bounded backpressure policy from Section 5.6.
        // Do NOT call accept() / onSnapshotBoundaryAccepted() on publish failure.
        return;
    }

    // 1. open the BOOK_UPDATE gate — parser now accepts live updates
    ctx.snapshotGatekeeper().accept();

    // 2. signal connector: Phase B complete — clears recoveryInProgress,
    //    increments recoveryCompletions counter. MUST follow accept().
    ctx.onSnapshotBoundaryAccepted();

    ctx.counters().snapshotsReceived().increment();
}
```

Failing to call `onSnapshotBoundaryAccepted()` leaves `recoveryInProgress = true`
permanently, blocking all future recovery requests for this instrument.

**Rule 7 — RecoveryStrategy calls `onChannelRestored()` exactly once on success.**

```java
// CORRECT
public void execute(RecoveryRequest request, RecoveryContext ctx) {
    try {
        performChannelWork(); // disconnect → reconnect → resubscribe
        ctx.onChannelRestored(); // exactly once — signals Phase A complete
    } catch (Exception e) {
        ctx.onRecoveryFailed(e.getMessage()); // exactly once on failure
    }
}

// WRONG — calling neither leaves recoveryInProgress stuck at true forever
// WRONG — calling both is a protocol violation
```

### 7.9 Ownership Reference Table

| Class | Owns as fields | Receives via context |
|---|---|---|
| `GatewayBootstrap` | `GatewayCounters`, `Publisher`, `EpochClock`, per-connector `CachedNanoClock`, per-connector `EventLoopGroup`, per-connector `ConnectorContext` | — |
| `AbstractConnector` | `encoder`, `publisher`, `counters`, `clock`, `parseCtx`, `snapshotCtx`, `recoveryCtx`, `recoveryInProgress`, all venue components | `ConnectorContext` in `init()` — unpacked immediately, NOT stored |
| `DefaultParseContext` | `encoder`, `publisher`, `counters`, `nanoClock`, `sequenceTracker`, `snapshotGatekeeper` | — |
| `DefaultSnapshotContext` | shared `snapshotGatekeeper`, `encoder`, `publisher`, `counters`, `nanoClock`, `onSnapshotAccepted` callback | — |
| `DefaultRecoveryContext` | `onChannelRestored` callback, `onFailed` callback, `counters` | — |
| `XFeedParser` | **nothing** | `ParseContext` per frame call |
| `XSnapshotStrategy` | **nothing** | `SnapshotContext` per snapshot call |
| `XRecoveryStrategy` | **nothing** | `RecoveryContext` per `execute()` call |
| `XSubscriptionBuilder` | venue credentials and config (final, from construction) | — |

---

## 8. Base Classes for Venue Developers

These five classes in `core/` eliminate boilerplate from every venue strategy.
A developer adding a new venue writes only the code specific to that exchange —
parser logic, auth, and subscribe payload construction. All lifecycle management,
context wiring, and recovery orchestration is handled by the base classes.

---

### 8.1 `AbstractConnector`

Owns all connector lifecycle: `init()`, `onTextFrame()`, `onBinaryFrame()`,
`recover()`, `shutdown()`, session context creation, and recovery completion
callbacks. Venue subclasses declare three abstract methods and write nothing else.

```java
// core/connector/AbstractConnector.java
/**
 * Base class for all venue connectors. Handles the complete connector lifecycle
 * so venue subclasses contain only exchange-specific declarations.
 *
 * A minimal venue connector looks like:
 *
 *   public final class CoinbaseL2Connector extends AbstractConnector {
 *       public CoinbaseL2Connector(InstrumentConfig i, FeedParser p,
 *                                   SnapshotStrategy s, RecoveryStrategy r,
 *                                   SubscriptionBuilder b) {
 *           super(i, p, s, r, b);
 *       }
 *       @Override protected VenueEnum venueEnum()     { return VenueEnum.COINBASE_L2; }
 *       @Override protected int       maxEntryCount() { return 10_000; }
 *       @Override protected int       headroomBytes() { return 8_192;  }
 *       @Override protected String    venueEndpoint() {
 *           return "wss://ws-feed.exchange.coinbase.com";
 *       }
 *   }
 */
public abstract class AbstractConnector implements Connector, WebSocketFrameHandler {

    // Subclass declares these — the only three required overrides
    protected abstract VenueEnum venueEnum();
    protected abstract int maxEntryCount();
    protected abstract int headroomBytes();

    private final FeedParser          parser;
    private final SnapshotStrategy    snapshot;
    private final RecoveryStrategy    recovery;
    private final SubscriptionBuilder subscription;
    private final InstrumentConfig    instrument;

    // Populated in init()
    protected Publisher           publisher;
    protected InstrumentCounters  counters;   // per-instrument counter set from GatewayCounters.forInstrument()
    protected NanoClock           clock;
    protected SbeEncoder          encoder;

    // Transport — constructed in init() so URI and SslContext are available;
    // connect() is called later by GatewayBootstrap after all connectors are init'd.
    private NettyWebSocketTransport transport;

    // Cached from TransportConfig in init() to avoid holding ConnectorContext beyond init().
    private long heartbeatTimeoutMs;

    private DefaultParseContext    parseCtx;
    private DefaultSnapshotContext snapshotCtx;
    private DefaultRecoveryContext recoveryCtx;

    // Recovery-in-progress flag — set in recover(), cleared in onSnapshotAccepted().
    // While set: duplicate recovery requests are coalesced (logged and ignored).
    // The flag spans both recovery phases: channel work AND snapshot acquisition.
    private boolean recoveryInProgress = false;

    // Shutdown flag — set by shutdown(); read by ReconnectRecoveryStrategy
    // doReconnect() loop to exit cleanly without calling onChannelRestored().
    // Written by the shutdown thread; read by the recovery thread.
    private volatile boolean shutdownRequested = false;

    protected AbstractConnector(InstrumentConfig instrument,
                                FeedParser parser,
                                SnapshotStrategy snapshot,
                                RecoveryStrategy recovery,
                                SubscriptionBuilder subscription) {
        this.instrument   = instrument;
        this.parser       = parser;
        this.snapshot     = snapshot;
        this.recovery     = recovery;
        this.subscription = subscription;
    }

    @Override
    public final void init(ConnectorContext ctx) {
        this.publisher          = ctx.publisher();
        this.counters           = ctx.counters().forInstrument(instrument);
        this.clock              = ctx.nanoClock();
        this.heartbeatTimeoutMs = ctx.transportConfig().heartbeatTimeoutMs();
        this.encoder = new SbeEncoder(
            instrument.instrumentId(),
            venueEnum().byteValue(),
            venueEnum().bookDepth().byteValue(),
            venueEnum().templateId().byteValue(),
            maxEntryCount(),
            headroomBytes()
        );
        // SslContext is built once here and reused across reconnects.
        // SslContext is thread-safe. See buildSslContext() below.
        SslContext sslContext = buildSslContext(ctx);
        this.transport = new NettyWebSocketTransport(
            ctx.eventLoopGroup(),
            URI.create(venueEndpoint()),
            sslContext,
            pipelineCustomizer(),
            ctx.transportConfig().frameSizeLimitBytes(),
            ctx.transportConfig().connectTimeoutMs(),
            this   // AbstractConnector implements WebSocketFrameHandler
        );
        buildSessionContexts();
    }

    /**
     * Returns the WebSocket endpoint URI string for this connector.
     * Implemented by the venue subclass using its typed config object
     * (e.g. CoinbaseConfig.endpoint()). AbstractConnector must not parse
     * the raw venueConfig map itself.
     */
    protected abstract String venueEndpoint();

    /**
     * Builds the BoringSSL SslContext for TLS.
     * Override in tests to return an insecure context without a real cert chain.
     *
     * Production pattern:
     *   SslContextBuilder.forClient()
     *       .sslProvider(SslProvider.OPENSSL)   // requires netty-tcnative-boringssl-static
     *       .build();
     *
     * SslContext is thread-safe and reusable across reconnects.
     */
    protected SslContext buildSslContext(ConnectorContext ctx) {
        try {
            return SslContextBuilder.forClient()
                .sslProvider(SslProvider.OPENSSL)
                .build();
        } catch (SSLException e) {
            throw new IllegalStateException("Failed to build BoringSSL SslContext", e);
        }
    }

    /** Override to add venue-specific Netty pipeline handlers. Default: no-op. */
    protected PipelineCustomizer pipelineCustomizer() {
        return pipeline -> {};
    }

    @Override public final void onTextFrame(ByteBuf frame)   { parser.onTextFrame(frame, parseCtx);  }
    @Override public final void onBinaryFrame(ByteBuf frame) { parser.onBinaryFrame(frame, parseCtx); }

    /** Called by NettyWebSocketTransport on the event-loop thread when connection drops unexpectedly. */
    @Override
    public final void onDisconnected(Throwable cause) {
        counters.connectionFailures().increment();
        recover(new RecoveryRequest(
            venueEnum(), instrument.instrumentId(),
            RecoveryRequestType.RESET, RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
            clock.nanoTime()));
    }

    @Override
    public final void connect() {
        try {
            transport.connect();                       // blocks until WS upgrade completes
            counters.connectionSuccesses().increment();
            counters.activeConnections().increment();
            sendSubscribe();
            // Seed the liveness clock so the first check doesn't spuriously fire
            counters.lastHeartbeatReceivedNanos().set(clock.nanoTime());
            startLivenessChecker();
        } catch (IOException e) {
            counters.connectionFailures().increment();
            // AbstractConnector does not retry here — GatewayBootstrap treats
            // a failed initial connect as a fatal startup error and exits.
            throw new IllegalStateException("Initial connect failed for " + instrument, e);
        }
    }

    @Override
    public final void recover(RecoveryRequest request) {
        if (recoveryInProgress) {
            counters.recoveryRequestsIgnoredInProgress().increment();
            return; // coalesce — do not trigger a second recovery while one is running
        }
        recoveryInProgress = true;
        publisher.publishReset(
            instrument.instrumentId(),
            venueEnum().byteValue(),
            venueEnum().bookDepth().byteValue(),
            venueEnum().templateId().byteValue(),
            clock);
        counters.recoveryAttempts().increment();
        recovery.execute(request, recoveryCtx);
    }

    @Override
    public final void shutdown() {
        shutdownRequested = true;    // signals doReconnect() loop to exit
        sendUnsubscribe();
        publisher.publishReset(
            instrument.instrumentId(),
            venueEnum().byteValue(),
            venueEnum().bookDepth().byteValue(),
            venueEnum().templateId().byteValue(),
            clock);
        transport.close();
        counters.activeConnections().decrement(); // gauge: connection closed
    }

    private void startLivenessChecker() {
        transport.eventLoop().scheduleAtFixedRate(
            this::checkLiveness,
            heartbeatTimeoutMs,
            heartbeatTimeoutMs / 2,
            TimeUnit.MILLISECONDS
        );
    }

    @Override public final VenueEnum venue()        { return venueEnum();               }
    @Override public final int       instrumentId() { return instrument.instrumentId();  }

    protected final void sendSubscribe() {
        byte[] payload = subscription.buildSubscribe(instrument);
        transport.sendText(payload);
    }

    protected final void sendUnsubscribe() {
        byte[] payload = subscription.buildUnsubscribe(instrument);
        transport.sendText(payload);
    }

    /**
     * Called by RecoveryContext.onChannelRestored() when the RecoveryStrategy
     * has completed channel work (disconnect → reconnect → resubscribe).
     *
     * This is Phase A of recovery. The snapshot has NOT yet arrived.
     * recoveryInProgress remains TRUE — it clears only in onSnapshotAccepted().
     *
     * Resets session state, rebuilds context objects, triggers snapshot acquisition.
     * Does NOT increment recoveryCompletions — that happens in onSnapshotAccepted().
     */
    private void onChannelRestored() {
        // Step 1: reset session-scoped state inside the existing ParseContext.
        // SequenceTracker and SnapshotGatekeeper renewed; ParseContext object unchanged.
        parseCtx.resetSession();

        // Step 2: rebuild SnapshotContext pointing at the new SnapshotGatekeeper.
        // Pass this::onSnapshotAccepted so it fires when the snapshot arrives.
        this.snapshotCtx = new DefaultSnapshotContext(
            parseCtx.currentSnapshotGatekeeper(), encoder, publisher, counters,
            clock,
            this::onSnapshotAccepted);

        // Step 3: rebuild RecoveryContext for the next recovery attempt.
        this.recoveryCtx = new DefaultRecoveryContext(
            this::onChannelRestored, this::onRecoveryFailed, counters);

        // Step 4: resubscribe and start snapshot acquisition.
        // For SUBSCRIBE_DRIVEN: snapshot will arrive via the feed.
        // For REST_THEN_DELTA: async REST fetch begins now.
        sendSubscribe();
        snapshot.triggerSnapshot(instrument, snapshotCtx);
        // recoveryInProgress remains true — cleared when snapshot is accepted.
    }

    /**
     * Called by DefaultSnapshotContext.onSnapshotBoundaryAccepted() when the
     * snapshot strategy has published BOOK_SNAPSHOT and opened the gatekeeper.
     *
     * This is Phase B of recovery — the genuinely complete event.
     * Clears recoveryInProgress and increments the recoveryCompletions counter.
     */
    private void onSnapshotAccepted() {
        recoveryInProgress = false;
        counters.recoveryCompletions().increment();
    }

    private void onRecoveryFailed(String reason) {
        recoveryInProgress = false;
        counters.recoveryFailures().increment();
        // schedule retry with exponential backoff
    }

    /**
     * Creates all three session context objects for the first time.
     * Called ONCE from init() only.
     *
     * On recovery, onChannelRestored() rebuilds snapshotCtx and recoveryCtx.
     * ParseContext is reset in place via resetSession(), never replaced.
     *
     * INVARIANT: parseCtx and snapshotCtx share the SAME SnapshotGatekeeper.
     */
    private void buildSessionContexts() {
        this.parseCtx    = new DefaultParseContext(encoder, publisher, counters, clock);
        this.snapshotCtx = new DefaultSnapshotContext(
            parseCtx.currentSnapshotGatekeeper(), encoder, publisher, counters,
            clock,
            this::onSnapshotAccepted);
        this.recoveryCtx = new DefaultRecoveryContext(
            this::onChannelRestored, this::onRecoveryFailed, counters);
    }
}
```

---

### 8.2 `AbstractConnectorFactory`

Handles config parsing and delegates wiring to the subclass. Venue factories
implement only the two exchange-specific steps.

```java
// core/connector/AbstractConnectorFactory.java
/**
 * Base class for all venue connector factories. Handles config parsing delegation.
 *
 * A minimal venue factory looks like:
 *
 *   public final class CoinbaseL2ConnectorFactory extends AbstractConnectorFactory {
 *       @Override public VenueEnum venue() { return VenueEnum.COINBASE_L2; }
 *
 *       @Override
 *       protected CoinbaseConfig parseVenueConfig(Map<String, Object> raw) {
 *           return CoinbaseConfig.parse(raw);
 *       }
 *
 *       @Override
 *       protected Connector createConnector(InstrumentConfig instrument,
 *                                           GatewayConfig config,
 *                                           Object venueConfig) {
 *           CoinbaseConfig cfg = (CoinbaseConfig) venueConfig;
 *           CoinbaseAuthenticator auth = new CoinbaseAuthenticator(cfg);
 *           return new CoinbaseL2Connector(
 *               instrument,
 *               new CoinbaseL2FeedParser(),
 *               new CoinbaseL2SnapshotStrategy(),
 *               new CoinbaseL2RecoveryStrategy(auth),
 *               new CoinbaseL2SubscriptionBuilder(auth)
 *           );
 *       }
 *   }
 */
public abstract class AbstractConnectorFactory implements ConnectorFactory {

    @Override
    public final Connector create(InstrumentConfig instrument, GatewayConfig config) {
        Object venueConfig = parseVenueConfig(config.venueConfig);
        return createConnector(instrument, config, venueConfig);
    }

    /**
     * Parse the raw venue-specific config block into a typed config object.
     * Called once per instrument at startup — allocation is fine here.
     */
    protected abstract Object parseVenueConfig(Map<String, Object> rawConfig);

    /**
     * Create and wire all venue-specific components. Return the ready connector.
     * The venueConfig is the typed object returned by parseVenueConfig().
     */
    protected abstract Connector createConnector(InstrumentConfig instrument,
                                                 GatewayConfig config,
                                                 Object venueConfig);
}
```

---

### 8.3 `SubscribeDrivenSnapshotStrategy`

For venues where the snapshot arrives automatically on subscribe (e.g. Coinbase L2,
Kraken L2). `triggerSnapshot()` is a no-op — the parser calls
`ctx.snapshotGatekeeper().accept()` when the snapshot frame arrives.

```java
// core/snapshot/SubscribeDrivenSnapshotStrategy.java
/**
 * Base for SUBSCRIBE_DRIVEN snapshot strategies.
 * The snapshot arrives via the normal feed after subscribe.
 * No explicit request is needed — triggerSnapshot() is a no-op.
 *
 * The venue's FeedParser is responsible for recognizing the snapshot frame
 * and calling ctx.snapshotGatekeeper().accept() when it arrives.
 *
 * A complete SUBSCRIBE_DRIVEN strategy is one line:
 *
 *   public final class CoinbaseL2SnapshotStrategy
 *       extends SubscribeDrivenSnapshotStrategy { }
 */
public abstract class SubscribeDrivenSnapshotStrategy implements SnapshotStrategy {

    @Override
    public final Mode mode() { return Mode.SUBSCRIBE_DRIVEN; }

    @Override
    public final void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
        // No-op. The snapshot arrives via the normal WebSocket feed.
        // The parser calls ctx.snapshotGatekeeper().accept() when it arrives.
    }
}
```

---

### 8.4 `ReconnectRecoveryStrategy`

For venues that recover by disconnecting and reconnecting (the most common pattern).
Subclasses implement four steps; the base handles the signal contract and counters.

```java
// core/recovery/ReconnectRecoveryStrategy.java
/**
 * Base for recovery strategies that use a full disconnect/reconnect cycle.
 * Handles the RecoveryContext signal contract and counter increments.
 * Subclasses implement the four venue-specific steps.
 *
 * A typical reconnect recovery strategy looks like:
 *
 *   public final class CoinbaseL2RecoveryStrategy extends ReconnectRecoveryStrategy {
 *       private final CoinbaseAuthenticator auth;
 *
 *       public CoinbaseL2RecoveryStrategy(CoinbaseAuthenticator auth) {
 *           this.auth = auth;
 *       }
 *
 *       @Override protected void doUnsubscribe() { channel.sendUnsubscribe(auth); }
 *       @Override protected void doDisconnect()  { channel.close(); }
 *       @Override protected void doReconnect()   { channel.connect(); }
 *       @Override protected void doResubscribe() { channel.sendSubscribe(auth, "level2", "heartbeat"); }
 *   }
 */
public abstract class ReconnectRecoveryStrategy implements RecoveryStrategy {

    @Override
    public final void execute(RecoveryRequest request, RecoveryContext ctx) {
        try {
            ctx.counters().recoveryExecutions().increment();
            doUnsubscribe();
            doDisconnect();
            doReconnect();
            doResubscribe();
            ctx.onChannelRestored(); // signals connector: channel work done, start snapshot
        } catch (Exception e) {
            ctx.counters().recoveryFailures().increment();
            ctx.onRecoveryFailed(e.getMessage());
        }
    }

    /** Send best-effort unsubscribe before closing. May be a no-op if already disconnected. */
    protected abstract void doUnsubscribe();

    /** Close the WebSocket connection. */
    protected abstract void doDisconnect();

    /** Reconnect with exponential backoff + jitter. Blocks until connected or throws. */
    protected abstract void doReconnect();

    /** Send authenticated subscribe for the channel(s) this strategy serves. */
    protected abstract void doResubscribe();
```

#### Reconnect Backoff Algorithm

`doReconnect()` implementations must follow this exact formula using the config
values `reconnectBackoffBaseMs`, `reconnectBackoffMaxMs`, and `reconnectBackoffJitterMs`:

```java
// Standard implementation — call this from doReconnect()
protected void reconnectWithBackoff() throws InterruptedException {
    int attempt = 0;
    while (!shutdownRequested) {
        long delay = Math.min(base * (1L << attempt), max)
                   + ThreadLocalRandom.current().nextLong(0, jitter);
        Thread.sleep(delay);
        try {
            transport.connect();
            attempt = 0;   // reset on success — next recovery starts fresh
            return;
        } catch (IOException e) {
            attempt = Math.min(attempt + 1, 10); // cap exponent to avoid overflow
            counters.reconnectAttempts().increment();
        }
    }
}
```

With default config values (`base=500ms`, `max=30000ms`, `jitter=250ms`):
```
attempt=0  →  sleep  500– 750ms
attempt=1  →  sleep 1000–1250ms
attempt=2  →  sleep 2000–2250ms
attempt=3  →  sleep 4000–4250ms
attempt=4  →  sleep 8000–8250ms
attempt=5  →  sleep 16000–16250ms
attempt=6+ →  sleep 30000–30250ms  (capped)
```

There is no maximum attempt count — retry continues until success or shutdown.
The `shutdownRequested` flag is a `volatile boolean` owned by `AbstractConnector`
and read by `doReconnect()` on each iteration. When set, `doReconnect()` returns
without calling `ctx.onChannelRestored()`, allowing the connector to shut down cleanly.

Shared utility methods for parsing JSON from Netty `ByteBuf`. All venue parsers
use these rather than re-implementing low-level scanning independently.

```java
// core/parser/ByteBufScanner.java
/**
 * Allocation-free utility methods for scanning JSON from a Netty ByteBuf.
 * All methods operate on the buffer's current reader index.
 * Thread-safe — all methods are stateless.
 *
 * Venue parsers use these to avoid re-implementing byte scanning, decimal
 * parsing, timestamp conversion, and array traversal per exchange.
 */
public final class ByteBufScanner {

    private ByteBufScanner() {}

    /**
     * Advances the reader index past the next occurrence of the ASCII key
     * (including its surrounding quotes and the following colon).
     * Returns true if found; false if the key was not found before the buffer end.
     * Does not allocate.
     */
    public static boolean scanToKey(ByteBuf buf, byte[] asciiKey) { ... }

    /**
     * Reads a quoted string value at the current reader position into dest[].
     * Returns the number of bytes written. Does not allocate a String.
     * Leaves the reader index after the closing quote.
     */
    public static int readStringInto(ByteBuf buf, byte[] dest) { ... }

    /**
     * Parses a quoted or unquoted decimal ASCII string from the buffer directly
     * into a (mantissa, scale) pair compatible with SbeEncoder.writeLevel() and
     * writeOrder(). Does not allocate. Never calls Double.parseDouble or any
     * floating-point operation.
     *
     * WHY NOT Double.parseDouble:
     *   Floating-point conversion can silently lose precision for large prices or
     *   quantities. "21921.73" survives, but values like "123456789.12345678"
     *   may round to a different double and produce a wrong mantissa after scaling.
     *   Direct integer scanning is lossless, faster, and allocation-free.
     *
     * ALGORITHM — implemented entirely with integer arithmetic:
     *   1. Skip opening quote if present (JSON numbers in arrays are unquoted;
     *      JSON string values are quoted).
     *   2. Read an optional leading '-' sign. Set negative = true if present.
     *   3. Scan integer digits, accumulating: mantissa = mantissa * 10 + digit.
     *   4. If '.' is encountered, continue scanning digits the same way,
     *      incrementing scale by 1 for each fractional digit.
     *   5. Stop at the first non-digit character ('"', ',', ']', '}', whitespace).
     *   6. If negative, negate mantissa.
     *   7. Trailing zeros after the decimal point are NOT stripped — scale
     *      reflects the exact number of digits in the source string.
     *      "0.06317902" → mantissa=6317902, scale=8
     *      "0.00000000" → mantissa=0,       scale=8
     *      "21922.10"   → mantissa=2192210,  scale=2
     *
     * UNSUPPORTED: scientific notation (e.g. "2.192173E4", "1e-8") is NOT supported
     *   and must be treated as a malformed value. If 'E' or 'e' is encountered
     *   during scanning, write mantissa = Long.MAX_VALUE and scale = 0 into result[],
     *   increment the malformed-rejections counter via the caller, and return.
     *   Reason: Coinbase prices are always fixed-point decimal strings. A future
     *   venue that uses scientific notation requires a separate parser, not an
     *   extension of this method.
     *
     * OVERFLOW GUARD:
     *   A price or quantity string that would overflow a signed long is a malformed
     *   message. If mantissa would exceed Long.MAX_VALUE during accumulation, write
     *   mantissa = Long.MAX_VALUE and scale = 0 into result[], increment the
     *   malformed-rejections counter via the caller, and return. The caller must
     *   check for this sentinel and reject the message without publishing.
     *
     * RESULT:
     *   result[0] = mantissa (signed long)
     *   result[1] = scale    (number of digits after the decimal point, 0–18)
     *
     * EXAMPLES:
     *   "21921.73"     → mantissa=2192173,    scale=2
     *   "0.06317902"   → mantissa=6317902,    scale=8
     *   "0"            → mantissa=0,           scale=0
     *   "100"          → mantissa=100,         scale=0
     *   "-0.5"         → mantissa=-5,          scale=1
     *   "21922.10"     → mantissa=2192210,     scale=2  (trailing zero retained)
     *
     * LEAVES the reader index at the first non-digit character after the number.
     * The caller is responsible for advancing past any closing quote if present.
     */
    public static void parseDecimal(ByteBuf buf, long[] result) { ... }

    /**
     * Reads an ASCII integer at the current reader position.
     * Does not allocate. Stops at the first non-digit character.
     */
    public static long readLong(ByteBuf buf) { ... }

    /**
     * Advances past whitespace and the current array element separator.
     * Returns true if positioned at the start of the next element;
     * returns false if the array end (']') has been reached.
     */
    public static boolean nextArrayElement(ByteBuf buf) { ... }

    /**
     * Parses an RFC 3339 / ISO 8601 UTC timestamp (e.g. "2023-02-09T20:33:09.999999Z")
     * to epoch nanoseconds. Does not allocate Instant, OffsetDateTime, or any
     * java.time object. Handles variable fractional-second precision.
     * Returns -1 if the value is absent, null, empty, or epoch-zero ("1970-01-01T00:00:00Z").
     */
    public static long parseTimestampNanos(ByteBuf buf) { ... }

    /**
     * Skips the current JSON value (string, number, object, or array) without parsing it.
     * Used to advance past fields the parser does not need.
     */
    public static void skipValue(ByteBuf buf) { ... }
}
```

---

### 8.6 What a Venue Developer Actually Writes

Using the base classes, adding `BINANCE_L2` requires exactly these classes
and approximately this much code:

| Class | Extends | Lines of code | What it contains |
|---|---|---|---|
| `BinanceL2Connector` | `AbstractConnector` | ~12 | Four `@Override` declarations: `venueEnum()`, `maxEntryCount()`, `headroomBytes()`, `venueEndpoint()` |
| `BinanceL2ConnectorFactory` | `AbstractConnectorFactory` | ~20 | Config parsing + component wiring |
| `BinanceL2FeedParser` | — (implements `FeedParser`) | ~80 | All genuine parse logic |
| `BinanceL2SnapshotStrategy` | — (implements `SnapshotStrategy`) | ~60 | REST fetch + delta alignment |
| `BinanceL2RecoveryStrategy` | `ReconnectRecoveryStrategy` | ~20 | Four `doX()` overrides |
| `BinanceL2SubscriptionBuilder` | — (implements `SubscriptionBuilder`) | ~30 | HMAC auth + JSON construction |
| `BinanceConfig` | — | ~10 | Config field declarations |

The parser, snapshot strategy, and subscription builder contain all the
genuine exchange-specific work. The connector, factory, and recovery strategy
are structural declarations with minimal code. A developer adding a new venue
spends their time exclusively on the three classes that actually need it.

---

## 9. Gateway Configuration

### 9.1 YAML Schema

```yaml
# gateway.yaml — one file per gateway instance
# All top-level keys map directly to GatewayConfig fields.
# SnakeYAML deserializes this flat structure into GatewayConfig without
# any custom mapping — key names must match Java field names exactly.

instanceId: "gateway-coinbase-l2-1"
environment: "prod"

# venue encodes both the exchange AND the data level as a single atomic identity.
# COINBASE_L2  → Coinbase level2 channel (aggregated depth, SUBSCRIBE_DRIVEN snapshot)
# COINBASE_L3  → Coinbase full channel   (individual orders, REST_THEN_DELTA snapshot)
# BINANCE_L2   → Binance depth stream    (aggregated depth, REST_THEN_DELTA snapshot)
venue: COINBASE_L2

instruments:
  - exchangeSymbol: "BTC-USD"
    instrumentId: 1001
  - exchangeSymbol: "ETH-USD"
    instrumentId: 1002
  - exchangeSymbol: "SOL-USD"
    instrumentId: 1003

transport:
  connectTimeoutMs: 5000
  reconnectBackoffBaseMs: 500
  reconnectBackoffMaxMs: 30000
  reconnectBackoffJitterMs: 250
  heartbeatTimeoutMs: 10000
  frameSizeLimitBytes: 4194304         # 4 MiB
  busyWaitEnabled: false               # true only with CPU pinning
  cpuAffinity:
    enabled: false                      # optional; pins only hot-path event-loop threads
    mode: AUTO                          # AUTO or EXPLICIT
    eventLoopCpuIds: []                 # required only when mode is EXPLICIT
  shutdownDeadlineMs: 5000             # max time to wait for graceful shutdown

encoding:
  maxLevelsPerMessage: 10000
  bufferHeadroomBytes: 8192

observability:
  countersSharedMemoryPath: "/dev/shm/gateway-coinbase-l2-1-counters"
  errorLogPath: "/dev/shm/gateway-coinbase-l2-1-errors"
  metricsHttpPort: 9090

# Publisher selection — controls how encoded SBE messages are delivered downstream.
#
# type: LOGGING       — integration validation adapter; writes structured summaries to a
#                       rolling file. Use for integration validation before a real
#                       downstream (Aeron, Chronicle, Kafka) is connected.
# type: IN_MEMORY     — captures messages in a ring buffer; for tests only.
# type: SHARED_MEMORY — future: Agrona off-heap IPC ring for co-located consumers.
# type: AERON        — future: Aeron reliable UDP publication.
# type: CHRONICLE    — future: Chronicle Queue for persistence + fan-out.
#
# Use LOGGING for integration validation before a real downstream is connected.
publisher:
  type: LOGGING
  logging:
    outputPath: "/var/log/gateway/messages.log"  # rolling log file path
    rollSizeMb: 256                               # rotate when file exceeds this size

# Venue-specific block — key must match venue name in lowercase (e.g. "coinbase").
# Parsed by the ConnectorFactory as a Map<String,Object> and cast to a typed
# config object (e.g. CoinbaseConfig) by the venue-specific parseVenueConfig() method.
# GatewayConfig.venueConfig holds the raw Map for the block whose key matches
# the lowercase prefix of the configured venue (COINBASE_L2 → "coinbase").
coinbase:
  # Coinbase Exchange Direct Feed — institutional WebSocket API
  endpoint: "wss://ws-feed.exchange.coinbase.com"
  apiKey: "${COINBASE_API_KEY}"
  passphrase: "${COINBASE_PASSPHRASE}"
  apiSecret: "${COINBASE_API_SECRET}"
  channels:
    - "level2"
    - "heartbeat"
```

### 9.2 Configuration Rules

- All YAML top-level keys map directly to `GatewayConfig` field names — the file is flat, not nested under a `gateway:` block
- `venue` must be exactly one recognized `VenueEnum` value
- `instruments` must be non-empty
- No two instruments may share the same `exchangeSymbol` or `instrumentId`
- The venue-specific YAML block key must match the lowercase prefix of the configured venue:
  `COINBASE_L2` → `coinbase:`, `BINANCE_L2` → `binance:`. Missing key is a startup failure.
- Credentials are resolved from environment variables at startup; empty resolution causes startup failure with a structured error that does not log the value
- `encoding.maxLevelsPerMessage` must be consistent with the encode buffer capacity formula in Section 17
- Configuration is parsed once at startup using **SnakeYAML**; SnakeYAML must not be imported into any hot-path class
- `transport.cpuAffinity` is optional. If absent, treat it as `enabled=false`.
- `transport.cpuAffinity.mode` must be `AUTO` or `EXPLICIT`; default to `AUTO` when affinity is enabled and mode is absent.
- `transport.cpuAffinity.eventLoopCpuIds` is required only when mode is `EXPLICIT`; its CPU ids must be non-negative and unique.
- If `transport.busyWaitEnabled=true`, `transport.cpuAffinity.enabled` must also be true so busy waiting is never enabled without thread pinning.
- Gateway refuses to start on any validation failure

### 9.3 Java Configuration Model

```java
// core/config/GatewayConfig.java
// SnakeYAML maps YAML top-level keys directly to these field names.
// Field names must match YAML keys exactly (case-sensitive).
public final class GatewayConfig {
    public String               instanceId;    // maps from YAML: instanceId
    public String               environment;   // maps from YAML: environment
    public VenueEnum            venue;         // maps from YAML: venue (e.g. COINBASE_L2)
    public List<InstrumentConfig> instruments; // maps from YAML: instruments list
    public TransportConfig      transport;     // maps from YAML: transport block
    public EncodingConfig       encoding;      // maps from YAML: encoding block
    public ObservabilityConfig  observability; // maps from YAML: observability block
    public PublisherConfig      publisher;     // maps from YAML: publisher block
    public Map<String, Object>  venueConfig;   // populated by GatewayBootstrap after parsing
                                               // NOT a direct YAML field — see note below
}
```

**`venueConfig` population rule:** SnakeYAML does not know to map the `coinbase:` YAML
block to `venueConfig`. `GatewayBootstrap` handles this in two steps:

```java
// Step 1: parse the full YAML into a raw Map<String, Object>
Map<String, Object> raw = new Yaml().load(yamlInputStream);

// Step 2: bind known top-level keys into GatewayConfig fields
GatewayConfig config = new GatewayConfig();
config.instanceId   = (String)        raw.get("instanceId");
config.environment  = (String)        raw.get("environment");
config.venue        = VenueEnum.valueOf((String) raw.get("venue"));
// ... bind transport, encoding, observability similarly ...

// Step 3: extract the venue-specific block using the lowercase venue name prefix.
// COINBASE_L2 → prefix "coinbase" → looks up raw.get("coinbase")
String venueKey = config.venue.name().split("_")[0].toLowerCase(); // "coinbase"
config.venueConfig = (Map<String, Object>) raw.get(venueKey);
if (config.venueConfig == null) {
    throw new IllegalArgumentException(
        "Missing venue config block '" + venueKey + "' in gateway.yaml");
}
```

```java
// core/config/InstrumentConfig.java
public final class InstrumentConfig {
    public String exchangeSymbol;  // e.g. "BTC-USD"
    public int    instrumentId;    // stable internal ID encoded in SBE output
}

// core/config/TransportConfig.java
public final class TransportConfig {
    public int     connectTimeoutMs;
    public long    reconnectBackoffBaseMs;
    public long    reconnectBackoffMaxMs;
    public long    reconnectBackoffJitterMs;
    public long    heartbeatTimeoutMs;
    public int     frameSizeLimitBytes;
    public boolean busyWaitEnabled;
    public CpuAffinityConfig cpuAffinity;
    public long    shutdownDeadlineMs;
}

public final class CpuAffinityConfig {
    public boolean enabled;
    public CpuAffinityMode mode;        // AUTO or EXPLICIT
    public List<Integer> eventLoopCpuIds;
}

public enum CpuAffinityMode {
    AUTO,
    EXPLICIT
}

// core/config/EncodingConfig.java
public final class EncodingConfig {
    public int maxLevelsPerMessage;   // used by ConnectorFactory to size encoder buffer
    public int bufferHeadroomBytes;   // added to the encode buffer beyond the max message size
}

// core/config/ObservabilityConfig.java
public final class ObservabilityConfig {
    public String countersSharedMemoryPath;
    public String errorLogPath;
    public int    metricsHttpPort;
}

// core/config/PublisherConfig.java
/**
 * IMPORTANT: type is declared as String, not PublisherType enum.
 *
 * SnakeYAML binds enum fields before application validation runs. If type were
 * declared as PublisherType and an invalid value appeared in the YAML, SnakeYAML
 * would throw a YAMLException during binding — before GatewayBootstrap can produce
 * a clear, controlled IllegalArgumentException.
 *
 * By declaring type as String, the YAML binding always succeeds. resolvePublisher()
 * then calls PublisherType.valueOf(type) explicitly, catching IllegalArgumentException
 * and rethrowing with a message that names the valid values. This gives the spec
 * full ownership of every startup failure message related to publisher configuration.
 */
public final class PublisherConfig {
    public String                 type;     // parsed to PublisherType by resolvePublisher()
    public LoggingPublisherConfig logging;  // only used when type = "LOGGING"
}

// core/config/PublisherType.java
public enum PublisherType {
    LOGGING,       // integration validation adapter — structured log file
    IN_MEMORY,     // test adapter — pre-allocated ring buffer capture
    SHARED_MEMORY, // future — Agrona IPC ring for co-located consumers
    AERON,         // future — Aeron reliable UDP
    CHRONICLE      // future — Chronicle Queue
}

// core/config/LoggingPublisherConfig.java
public final class LoggingPublisherConfig {
    public String outputPath;   // path to rolling log file
    public int    rollSizeMb;   // rotate when file exceeds this size in MiB
}
```

---

## 10. Gateway Bootstrap and Venue Wiring

### 10.1 `VenueRegistry`

Uses Java `ServiceLoader` — contains **no imports from any `venue/` package**.

```java
// core/bootstrap/VenueRegistry.java
public final class VenueRegistry {

    private static final Map<VenueEnum, ConnectorFactory> REGISTRY;

    static {
        Map<VenueEnum, ConnectorFactory> map = new EnumMap<>(VenueEnum.class);
        for (ConnectorFactory factory : ServiceLoader.load(ConnectorFactory.class)) {
            if (map.put(factory.venue(), factory) != null) {
                throw new IllegalStateException(
                    "Duplicate ConnectorFactory for venue: " + factory.venue());
            }
        }
        REGISTRY = Collections.unmodifiableMap(map);
    }

    public static ConnectorFactory forVenue(VenueEnum venue) {
        ConnectorFactory f = REGISTRY.get(venue);
        if (f == null) throw new IllegalArgumentException(
            "No ConnectorFactory registered for venue: " + venue +
            ". Add a META-INF/services entry.");
        return f;
    }
}
```

### 10.2 Startup Sequence

```
1.  Load and validate GatewayConfig (JVM arg: -Dgateway.config=<path>)
      Fail fast: missing venue, empty instruments, duplicate IDs, empty credentials
2.  Initialize Agrona CountersManager → memory-mapped at countersSharedMemoryPath
3.  Initialize Agrona DistinctErrorLog → memory-mapped at errorLogPath
4.  Create shared gateway resources:
      GatewayCounters counters  = new GatewayCounters(countersManager, instanceId,
                                                       environment, venue)
      Publisher       publisher = resolvePublisher(config)
      EpochClock      epoch     = new SystemEpochClock()
5.  Resolve ConnectorFactory via VenueRegistry.forVenue(config.venue)
6.  For each InstrumentConfig:
      a. EventLoopGroup eventLoopGroup =
             Epoll.isAvailable()
                 ? new EpollEventLoopGroup(1, namedThreadFactory(instrument))
                 : new NioEventLoopGroup(1, namedThreadFactory(instrument))
      b. NanoClock nano = new CachedNanoClock()   // dedicated to this connector's event loop
      c. ConnectorContext ctx = new DefaultConnectorContext(
             counters, publisher, nano, epoch,
             config.transport,          // TransportConfig
             eventLoopGroup,            // dedicated group for this connector
             config.venueConfig         // raw Map<String,Object> venue block
         )
      d. Connector connector = factory.create(instrument, config)
      e. connector.init(ctx)            // allocates encoder, transport, contexts
      f. Retain `(connector, eventLoopGroup, nano)` for shutdown orchestration
7.  Start metrics HTTP thread on metricsHttpPort
8.  Register SIGTERM shutdown hook
9.  For each Connector: connector.connect()  // dial → TLS → WS upgrade → subscribe
10. Block main thread until shutdown signal
```

### `GatewayBootstrap` — Entry Point

`GatewayBootstrap` is the process entry point. It contains `public static void main(String[] args)`
and is declared as the JAR manifest `Main-Class`. There is no other entry point class.

```java
// core/bootstrap/GatewayBootstrap.java
public final class GatewayBootstrap {

    private static final Logger log = LoggerFactory.getLogger(GatewayBootstrap.class);

    /**
     * Process entry point. Declared as Main-Class in the JAR manifest.
     *
     * Configuration is supplied entirely via the JVM system property
     * {@code -Dgateway.config=<path>}. Command-line args are intentionally
     * ignored — all configuration lives in the YAML file.
     *
     * Exits with status 1 on any startup failure so the process supervisor
     * (systemd, Kubernetes, etc.) can detect and restart the process.
     */
    public static void main(String[] args) {
        String configPath = System.getProperty("gateway.config");
        if (configPath == null || configPath.isBlank()) {
            System.err.println("[FATAL] -Dgateway.config=<path> is required");
            System.exit(1);
        }
        try {
            new GatewayBootstrap().start(configPath);
        } catch (Exception e) {
            log.error("[FATAL] Startup failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Runs the full startup sequence (steps 1–10 above).
     * Blocks until SIGTERM is received and the graceful shutdown completes.
     *
     * @param configPath absolute or relative path to gateway.yaml
     * @throws Exception on any unrecoverable startup error
     */
    public void start(String configPath) throws Exception {
        // ... steps 1–10 as described in the sequence above ...
    }

    // --- private helpers ---

    private static GatewayConfig loadAndValidate(String configPath) { ... }
    private static Publisher resolvePublisher(GatewayConfig config) { ... }
    private static EventLoopGroup resolveEventLoopGroup(
            TransportConfig config, InstrumentConfig instrument) { ... }
    private static ThreadFactory namedThreadFactory(InstrumentConfig instrument) { ... }
}
```

**Key rules for `main()`:**
- `System.getProperty("gateway.config")` is the only supported configuration source.
  Command-line `args[]` are ignored. This keeps the launch command simple and unambiguous.
- Any unhandled exception from `start()` is logged with full stack trace, then
  `System.exit(1)` is called. This ensures the process supervisor sees a non-zero
  exit code and can restart automatically.
- `System.exit(0)` is called at the end of a clean graceful shutdown initiated by
  SIGTERM. The shutdown hook registered in step 8 calls the graceful shutdown
  sequence; `main()` blocks on a `CountDownLatch` that the hook releases when done.

### Launch Command

```bash
# Production launch
java \
  -Dgateway.config=/etc/gateway/gateway.yaml \
  -jar crypto-market-data-gateway.jar

# With explicit GC tuning for low-latency workloads (optional)
java \
  -Dgateway.config=/etc/gateway/gateway.yaml \
  -XX:+UseZGC \
  -XX:MaxGCPauseMillis=1 \
  -Xms512m -Xmx512m \
  -jar crypto-market-data-gateway.jar

# Sandbox / integration testing (short heartbeat timeout for fast test cycles)
java \
  -Dgateway.config=/etc/gateway/gateway-sandbox.yaml \
  -jar crypto-market-data-gateway.jar
```

The config file path is the **only required argument**. All other behaviour —
venue, instruments, publisher type, transport selection, observability paths —
is driven by the YAML file.

---

## 11. Key Constraints

- **Java 21**
- **Maven** project
- **Netty** for WebSocket transport — no Spring
- **Agrona** direct buffers for off-heap encoding and memory
- **SnakeYAML** for configuration parsing at startup only — never imported on hot path
- Netty transport must use **pooled direct `ByteBuf`** — no unpooled hot-path allocation
- **No Lombok**
- **No Jackson databind / Gson on hot path**
- **No reflection-based deserialization**
- **No regex / `String.split`** on hot path
- **No Java Streams** or allocation-prone functional abstractions on hot path
- **No boxing-heavy APIs** where primitive-specialized logic is practical
- Low allocation — not zero-copy end-to-end, not zero-GC, but minimized
- Hot path must remain off-heap wherever practical
- Hot-path behavior must be **deterministic** under sustained load
- `REST_THEN_DELTA` snapshot fetches use **Java 11 `java.net.http.HttpClient`**
  on a dedicated non-hot-path thread

### 11.1 Maven Dependency Versions

The following minimum versions are required. Use the most recent stable patch
within each minor version unless a specific bug fix demands a higher version.

| `groupId` | `artifactId` | Min version | Purpose |
|---|---|---|---|
| `io.netty` | `netty-all` | `4.1.100.Final` | WebSocket transport, ByteBuf, pipeline |
| `io.netty` | `netty-transport-native-epoll` | `4.1.100.Final` | Linux epoll event loop |
| `io.netty` | `netty-tcnative-boringssl-static` | `2.0.61.Final` | BoringSSL for TLS (`SslProvider.OPENSSL`) |
| `org.agrona` | `agrona` | `1.20.0` | `MutableDirectBuffer`, `AtomicCounter`, `CachedNanoClock`, `DistinctErrorLog`, `CountersManager` |
| `org.yaml` | `snakeyaml` | `2.2` | YAML config parsing — startup only |
| `net.openhft` | `affinity` | `3.23.3` | CPU thread pinning (`AffinityLock`) |
| `org.hdrhistogram` | `HdrHistogram` | `2.1.12` | Latency percentile tracking (Phase 3+) |
| `junit` | `junit-jupiter` | `5.10.0` | Unit and integration tests |
| `net.bytebuddy` | `byte-buddy-agent` | `1.14.9` | Allocation measurement in tests |

**Notes:**
- `netty-tcnative-boringssl-static` must match the Netty minor version exactly —
  version mismatch causes native library load failures at startup.
- `agrona` must be a single consistent version across the classpath — transitive
  version conflicts cause `UnsafeBuffer` binary incompatibility.
- `snakeyaml` 2.x has breaking API changes from 1.x — use 2.x exclusively.
- `net.bytebuddy:byte-buddy-agent` is a test-scope dependency only; it is used
  to measure heap allocation in hot-path tests without modifying production code.
- `HdrHistogram` is a runtime dependency from Phase 3 onwards; it may be
  `optional` in the pom during Phase 1 and 2.

### 11.2 Executable JAR Packaging

The gateway is packaged as a **single fat JAR** (all dependencies on the
classpath, `Main-Class` set in the manifest). The entry point class is always
`io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap`.

#### Maven — `maven-shade-plugin`

Add to `pom.xml` inside `<build><plugins>`:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>3.5.1</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals><goal>shade</goal></goals>
      <configuration>
        <createDependencyReducedPom>false</createDependencyReducedPom>
        <transformers>
          <transformer implementation=
            "org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
            <mainClass>
              io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap
            </mainClass>
          </transformer>
          <!-- Merge Netty native library index files -->
          <transformer implementation=
            "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
        <filters>
          <filter>
            <!-- Strip module-info and signature files that break shading -->
            <artifact>*:*</artifact>
            <excludes>
              <exclude>module-info.class</exclude>
              <exclude>META-INF/*.SF</exclude>
              <exclude>META-INF/*.DSA</exclude>
              <exclude>META-INF/*.RSA</exclude>
            </excludes>
          </filter>
        </filters>
      </configuration>
    </execution>
  </executions>
</plugin>
```

Build command:
```bash
mvn clean package -DskipTests
# produces: target/crypto-market-data-gateway-<version>.jar
```

#### Gradle (Kotlin DSL) — `application` plugin + `shadowJar`

```kotlin
// build.gradle.kts
plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

application {
    mainClass.set("io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap")
}

tasks.shadowJar {
    archiveClassifier.set("")           // overwrite the plain jar, not add a -all suffix
    mergeServiceFiles()                 // merge META-INF/services/* for ServiceLoader
    exclude("module-info.class")
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

// Make 'build' depend on shadowJar so `gradle build` produces the fat jar
tasks.build { dependsOn(tasks.shadowJar) }
```

```kotlin
// settings.gradle.kts
rootProject.name = "crypto-market-data-gateway"
```

Build command:
```bash
gradle shadowJar
# produces: build/libs/crypto-market-data-gateway-<version>.jar
```

**Notes:**
- `ServicesResourceTransformer` (Maven) and `mergeServiceFiles()` (Gradle) are
  **required**. Without them the `META-INF/services/...ConnectorFactory` file
  is overwritten rather than merged, breaking `ServiceLoader` discovery at runtime.
  The `VenueRegistry` static initializer will find no factories and every startup
  will fail with `IllegalArgumentException`.
- Netty native libraries (`netty-transport-native-epoll`, `netty-tcnative-boringssl-static`)
  are packaged as JNI resources inside the jar. No additional native library
  path configuration is needed at runtime.
- The `netty-transport-native-epoll` artifact requires the `linux-x86_64` classifier
  (or `linux-aarch_64` on ARM) to include native libraries. Without the classifier
  Maven/Gradle resolves an empty jar, causing `UnsatisfiedLinkError` at runtime.
  On macOS/Windows developer machines where epoll is unavailable the gateway
  falls back to NIO automatically — the empty jar resolves without error on those
  platforms.

---

## 12. Buffer and Memory Model

### Inbound Transport Buffer

- WebSocket payloads processed from **Netty pooled direct `ByteBuf`**
- Parsing occurs directly against the incoming `ByteBuf` — no heap copy
- Netty frame aggregation configured with explicit size bounds
- Frame size limit configurable via `transport.frameSizeLimitBytes`

### Encode Buffer

- Encoding uses an **off-heap direct `ByteBuffer`** wrapped by
  **Agrona `UnsafeBuffer` / `MutableDirectBuffer`**
- Each connector owns **one reusable encode buffer**, allocated in `init()`
- Buffer sized using the template-appropriate formula plus headroom:
  - `BOOK_LEVEL`:   `alignUp(8 + 47 + (maxEntryCount × 20) + headroomBytes, granularity)`
  - `ORDER_ENTRY`:  `alignUp(8 + 47 + (maxEntryCount × 29) + headroomBytes, granularity)`
- Example for `COINBASE_L2` (BOOK_LEVEL, 10,000 entries, 8,192 headroom):
  `8 + 47 + (10,000 × 20) + 8,192 = 208,247 bytes` → use 256 KiB
- Example for `COINBASE_L3` (ORDER_ENTRY, 10,000 entries, 8,192 headroom):
  `8 + 47 + (10,000 × 29) + 8,192 = 298,247 bytes` → use 512 KiB
- Reused across **1,000,000+ messages** with zero per-message allocation

### Reuse Rules

- Thread-affine: one writer, one buffer, one event-loop thread
- Publisher handoff must complete before the next `beginMessage()` call
- If downstream retention is required, use a bounded off-heap buffer ring or pool
- Recovery and control notifications must not cause hidden buffer retention

---

## 13. Parser Strategy

### Input
Parse directly from **Netty off-heap `ByteBuf`** — no heap copy of JSON payload.

### Method
Sequential byte scanning: detect keys via ASCII matching, parse values in place,
avoid materializing intermediate JSON structures.

### Numeric Parsing
Convert numeric strings to `(mantissa, scale)` pairs. Avoid `Double.parseDouble`.

### Arrays
Traverse JSON arrays manually. Extract only required structures (`bids`, `asks`, `changes`).

### Message Classification
Determined by the top-level `"type"` field (Coinbase) or equivalent venue discriminator.

### Sequence Mapping
Each venue's `FeedParser` is solely responsible for supplying `gatewayMessageSeq`,
`seq1`, and `seq2` to `SbeEncoder.beginMessage()`. Two patterns exist:

- **Gateway-managed** (Coinbase, Kraken): `long seq = ctx.sequenceTracker().next()`;
  pass `seq` for all three sequence arguments.
- **Exchange-native** (Binance): `long gSeq = ctx.sequenceTracker().next()`;
  read `seq1` and `seq2` from the exchange frame; pass all three separately.

See Section 6.2 (`SequenceTracker`) for per-venue examples.

### Allocation Policy

Unavoidable: some control strings may need stable representation outside the frame lifetime.

Avoided on hot path: no POJO mapping, no JSON trees, no intermediate containers,
no per-level objects, no reflection, no generic helper objects for malformed input.

### Future Optimization — SIMD JSON Parsing

For venues that send large snapshots (e.g. Binance full depth with 1,000+ levels),
handwritten scalar `ByteBuf` scanning is the correct baseline. Once the gateway
is operational and profiled, consider evaluating SIMD-accelerated JSON parsing
(e.g. `simdjson` via JNI, or a pure-JVM port) for the snapshot hot path.
SIMD parsing can be 3–5× faster than scalar scanning on large arrays.

This is a per-venue README item — not core infrastructure. The `FeedParser`
interface is unchanged; the implementation switches internally when a venue's
README documents the optimization has been validated for that feed.

---

## 14. Coinbase L2 Strategy — Supported Messages

This section covers the `COINBASE_L2` venue strategy only. It targets the
authenticated Coinbase Exchange Direct Feed `level2` channel. The `COINBASE_L3`
strategy targets the `full` channel and is defined in `venue/coinbase/l3/README.md`.

### Supported Message Set (COINBASE_L2)

- Outbound authenticated `subscribe` for `level2`
- Outbound authenticated `subscribe` for `heartbeat`
- Outbound authenticated `unsubscribe` on shutdown or recovery
- Inbound `snapshot` from `level2`
- Inbound `l2update` from `level2`
- Inbound `heartbeat`
- Inbound `subscriptions` acknowledgement (control-plane validation only)
- Inbound `error` (control-plane; authentication failure, rate-limit, invalid product)

### Authentication Scope

- Targets the **Coinbase Exchange Direct Feed** at `wss://ws-feed.exchange.coinbase.com`
  — the authenticated institutional Exchange WebSocket API
- Authentication uses the Exchange API HMAC-SHA256 signing scheme, equivalent
  to signing the string `GET /users/self/verify` with the API secret
- Required fields on every subscribe/unsubscribe message: `signature`, `key`, `passphrase`, `timestamp`
- `timestamp` is the current Unix epoch in seconds as a string
- `signature` = HMAC-SHA256(secret, timestamp + "GET" + "/users/self/verify"), base64-encoded
- Authentication is control-path work (not per-message), but latency-sensitive
  during recovery because it directly affects time-to-resubscribe
- Reuse pre-decoded credentials and bounded temporary buffers during signing
- Secrets must not appear in logs, metrics labels, or exception messages

### Sample Message Contract

Parser extracts required fields only. Field order is not assumed. Extra or unknown
fields are tolerated when required fields are present.

**Outbound Subscribe (single message — both channels)**

The gateway sends **one** subscribe message containing both `level2` and `heartbeat`
in the `channels` array. Sending them separately would produce two `subscriptions`
acknowledgements, and the first one would fail validation (only one channel confirmed)
and trigger spurious recovery.

```json
{
  "type": "subscribe",
  "product_ids": ["BTC-USD"],
  "channels": ["level2", "heartbeat"],
  "signature": "<sig>",
  "key": "<key>",
  "passphrase": "<passphrase>",
  "timestamp": "<epoch-seconds>"
}
```

**Outbound Unsubscribe** — same structure, `"type": "unsubscribe"`

**Inbound `subscriptions` Acknowledgement**

```json
{
  "type": "subscriptions",
  "channels": [
    { "name": "level2",    "product_ids": ["BTC-USD"] },
    { "name": "heartbeat", "product_ids": ["BTC-USD"] }
  ]
}
```

This is a control-plane message. It must never be encoded into book output.

**Validation rules:**

1. Locate the `channels` array in the payload.
2. For each expected channel name (`level2`, `heartbeat`), find a matching
   entry in `channels` where `name` equals the expected channel name.
3. Within that entry, verify that `product_ids` contains the connector's
   single subscribed `exchangeSymbol` (e.g. `"BTC-USD"`).
4. If both channels are confirmed with the correct product ID → validation passes.
5. If either channel is absent, or the product ID does not match → treat as a
   control-plane error: increment the subscription validation failure counter,
   emit a structured lifecycle log entry, and trigger recovery.
6. Extra channels in the `channels` array (beyond `level2` and `heartbeat`) are
   tolerated and ignored — do not fail on unexpected extra entries.
7. Validation is per-connection, not per-instrument. Each connector manages one
   connection for one instrument and validates its own ack independently.

**Inbound `level2` Snapshot**
```json
{
  "type": "snapshot",
  "product_id": "BTC-USD",
  "time": "2023-02-09T20:33:09.999999Z",
  "bids": [["21921.73", "0.06317902"]],
  "asks": [["21922.10", "0.42000000"]]
}
```
- `type = snapshot` → `eventType = BOOK_SNAPSHOT`
- `product_id` must resolve to configured `instrumentId`
- bid entries → `side = BID`, ask entries → `side = ASK`, all rows → `action = UPSERT`
- `exchangeTimestamp` rule: use the top-level `time` field if present and non-zero.
  The Coinbase Exchange Direct Feed includes a top-level `time` field on snapshot
  messages. If the field is absent, empty, or epoch-zero, encode `-1`.
  Do not use any per-level timestamp field.
- Sequence: no exchange seq in payload → `seq = ctx.sequenceTracker().next()`;
  `gatewayMessageSeq = seq1 = seq2 = seq`

**Inbound `level2` Update**
```json
{
  "type": "l2update",
  "product_id": "BTC-USD",
  "time": "2023-02-09T20:33:09.999999Z",
  "changes": [
    ["buy", "21921.73", "0.05000000"],
    ["sell", "21922.10", "0"]
  ]
}
```
- `type = l2update` → `eventType = BOOK_UPDATE`
- `"buy"` → `side = BID`, `"sell"` → `side = ASK`
- size `0` → `action = DELETE`, size `> 0` → `action = UPSERT`
- `time` → `exchangeTimestamp`
- No exchange seq → `seq = ctx.sequenceTracker().next()`; all three seq fields = seq

**Inbound `heartbeat`**
```json
{
  "type": "heartbeat",
  "sequence": 90,
  "product_id": "BTC-USD",
  "time": "2014-11-07T08:19:28.464459Z"
}
```
- Update connection-health state and observability counters
- Track time since last heartbeat for liveness timeout
- `sequence` field: captured for diagnostics only; not used as L2 book sequence
- Not encoded into the SBE book stream
- If heartbeat timeout elapses: increment liveness-failure counters → trigger recovery

### Heartbeat Liveness Timeout Algorithm

Timeout is checked using a scheduled task on the connector's event-loop thread,
not on every frame arrival. This avoids adding a time comparison to every hot-path
frame. The Netty `EventLoop` implements `ScheduledExecutorService` — use it
directly so `checkLiveness()` runs on the same thread as the hot path with no
synchronization needed.

```java
// Called from AbstractConnector after connect() succeeds.
// Must be scheduled from within the event-loop thread.
void startLivenessChecker() {
    transport.eventLoop().scheduleAtFixedRate(
        this::checkLiveness,
        heartbeatTimeoutMs,      // initial delay — allows first heartbeat to arrive
        heartbeatTimeoutMs / 2,  // check period — half the timeout for early detection
        TimeUnit.MILLISECONDS
    );
}

// checkLiveness() — runs on the event-loop thread:
void checkLiveness() {
    if (recoveryInProgress) return; // already recovering — skip this cycle
    long nowNanos  = nanoClock.nanoTime();
    long lastNanos = counters.lastHeartbeatReceivedNanos().get();
    long elapsedMs = (nowNanos - lastNanos) / 1_000_000L;

    if (elapsedMs >= heartbeatTimeoutMs) {
        counters.heartbeatsMissed().increment();
        counters.livenessFailures().increment();
        recover(new RecoveryRequest(venue, instrumentId,
                    RESET, HEARTBEAT_TIMEOUT, nowNanos));
    }
}

// Updated on every inbound heartbeat frame in the parser:
counters.lastHeartbeatReceivedNanos().set(nanoClock.nanoTime());
counters.heartbeatsReceived().increment();
```

**Clock source:** `CachedNanoClock` (from `ParseContext.nanoClock()`) —
not `System.nanoTime()` directly. The cached clock is refreshed once per
event-loop iteration, which is sufficient for a 10-second timeout check.

**Initial seed:** `lastHeartbeatReceivedNanos` is initialized to `nanoClock.nanoTime()`
at the point when the `subscriptions` ack validation passes. This prevents a
spurious liveness failure in the subscribe-to-first-heartbeat window.

**Inbound `error`**
```json
{
  "type": "error",
  "message": "Failed to subscribe",
  "reason": "authentication failure"
}
```

Coinbase sends this message on authentication failure, invalid product ID,
rate-limiting, and other server-side rejections. It is a control-plane message
and must never be encoded into book output.

**Handling rule:**
1. Increment `authenticationErrors` counter.
2. Emit a structured lifecycle log entry including `message` and `reason` fields
   (never include credentials in the log even if they appear in the error body).
3. Trigger recovery immediately via
   `ctx.requestRecovery(RESET, STREAM_INTEGRITY_FAILURE, reason)`.

**Why recovery and not silent drop:** an `error` frame means the current session
is permanently invalid — the exchange will not send any further market data on
this connection. Falling through to the `unknownTypeDrops` path would leave the
connector alive but receiving nothing, with no liveness event to trigger recovery
until the heartbeat timeout fires. Treating `error` as a distinct, immediate
recovery trigger eliminates that silent-dead-session window.

### Parsing Notes

- `product_id` must match the connector's single subscribed instrument; mismatch is
  a protocol violation — do not publish, increment error counter
- After subscribe or recovery resubscribe: drop all `l2update` frames until first
  accepted `snapshot` — drop immediately, do not buffer
- If stream-integrity failure, malformed payload, or liveness failure detected:
  self-trigger recovery without waiting for downstream request
- Parser must tolerate extra fields and must not depend on field ordering
- **Duplicate price levels within one `l2update`:** The Coinbase Exchange Direct Feed
  does not guarantee that price levels within a single `l2update.changes` array are
  unique. A single message may contain two or more entries with the same price.
  The gateway encodes all entries as-is in arrival order — it does not deduplicate.
  Downstream consumers must apply entries in the order they appear in the SBE
  repeating group. The last entry for a given price level takes precedence if the
  consumer maintains a per-level map. This must be documented in the downstream
  consumer's integration contract.

---

## 15. Threading Model

### Core Principle: One Connection, One Thread, One Instrument

One WebSocket connection per instrument is a **core architectural principle**,
not a Coinbase-specific choice. All venue implementations must follow it.

Rationale:
- Complete failure isolation per instrument
- Independent recovery per instrument
- Single-writer discipline — one encode buffer, one event-loop thread, no synchronization
- Simpler sequence tracking — state fully scoped to one connection
- CPU affinity — each hot-path thread pinnable independently

Venues that natively multiplex instruments on one connection must manage
multiplexing internally inside their `Connector`. The core gateway model
does not support shared connections.

### Netty Event Loop

- Each connection runs on its own hot-path event-loop thread
- All hot-path processing — parsing, encoding, publishing — happens on the I/O thread
- Event-loop threads may optionally be pinned to dedicated CPU cores (config-driven)
- Non-hot-path work (metrics, admin, REST fetches) runs on separate threads

### Execution Strategy

Parse → encode → publish stay on the same event-loop thread. No queues, no context
switching, no synchronization around shared buffers.

### Queues

Not used on the hot path. May exist inside future asynchronous publisher adapters.
Recovery signals from downstream are scheduled onto the owning event-loop thread
before execution — never executed directly from an off-thread caller.

---

## 16. Normalized Binary Model

### Sequence Fields

Every outbound message carries three sequence fields:

| Field | Source | Purpose |
|---|---|---|
| `gatewayMessageSeq` | Always `SequenceTracker.next()` | Per-session monotonic counter; detects gateway-level drops |
| `seq1` | Venue-specific (see table) | Start sequence; detects exchange-level gaps |
| `seq2` | Venue-specific (see table) | End sequence; detects exchange-level gaps |

`gatewayMessageSeq` and `seq1`/`seq2` serve different purposes and catch different
problems. A `gatewayMessageSeq` gap means the gateway dropped or failed to publish
a message. A `seq1`/`seq2` gap means the exchange feed had a hole even though the
gateway processed every frame it received.

**Sequence source by venue:**

| Venue | `seq1` source | `seq2` source | `seq1 == seq2` |
|---|---|---|---|
| Coinbase level2 | `sequenceTracker().next()` — gateway counter (no exchange seq) | same | Yes |
| Binance depth (future) | `U` field (firstUpdateId) from frame | `u` field (finalUpdateId) | No — covers a range |
| Kraken (future) | `sequenceTracker().next()` — gateway counter (checksum integrity) | same | Yes |

For Coinbase and Kraken, `gatewayMessageSeq = seq1 = seq2` — the same `next()` call
supplies all three. For Binance, `next()` supplies `gatewayMessageSeq` only; `seq1`
and `seq2` come from the exchange frame.

### Snapshot vs Incremental

- **`BOOK_SNAPSHOT`**: full book replace; all levels encode `action = UPSERT`;
  establishes the clean publish boundary for this session
- **`BOOK_UPDATE`**: incremental; `action = UPSERT` or `DELETE` per level;
  must not be published before the first accepted snapshot boundary

### Recovery Events

- **`BOOK_RESET`**: downstream must discard current state for this instrument
- **`BOOK_SNAPSHOT`**: downstream must treat as fresh authoritative book image
- **`BOOK_UPDATE`**: downstream applies incrementally after a valid snapshot boundary

Required ordering: `BOOK_RESET` → `BOOK_SNAPSHOT` → `BOOK_UPDATE`. No interleaving.

---

## 17. Binary Encoding Model

### Style
SBE-style (not generated SBE) — hand-written encoder and decoder.

### Layout

```
[Header]     magic, version, templateId, blockLength, entryCount
[Body]       eventType, venue, bookDepth, instrumentId,
             gatewayMessageSeq, seq1, seq2,
             exchangeTimestamp, ingressTimestamp
[Repeating]  one entry per level (BOOK_LEVEL template)
             or one entry per order (ORDER_ENTRY template)
```

### Field Sizes — Little-Endian

**Header (8 bytes total)**

| Field | Type | Bytes | Notes |
|---|---|---|---|
| `magic` | `uint16` | 2 | Fixed protocol marker — value `0xEB0B` (little-endian: `0B EB`). Decoders must reject any message where this field is not `0xEB0B`. |
| `version` | `uint8` | 1 | Schema version — initial value `1`. Decoders must reject messages with `version > known_version`. Backward compatibility is not required; a schema change increments `version` and requires coordinated deployment. |
| `templateId` | `uint8` | 1 | BOOK_LEVEL=1, ORDER_ENTRY=2 — determines repeating group layout |
| `blockLength` | `uint16` | 2 | Fixed body length; use to locate repeating group |
| `entryCount` | `uint16` | 2 | Number of repeating group entries — written by `SbeEncoder.endMessage()` for normal encoded messages, or written directly by the publisher reset encoder for `BOOK_RESET` |

**Body (47 bytes total)**

| Field | Type | Bytes | Notes |
|---|---|---|---|
| `eventType` | `uint8` | 1 | BOOK_RESET=1, BOOK_SNAPSHOT=2, BOOK_UPDATE=3 |
| `venue` | `uint8` | 1 | See Section 23 for allocation table |
| `bookDepth` | `uint8` | 1 | L1=1, L2=2, L3=3 — depth level of this message |
| `instrumentId` | `uint32` | 4 | Stable configured internal ID |
| `gatewayMessageSeq` | `uint64` | 8 | Gateway monotonic per-session counter |
| `seq1` | `uint64` | 8 | Start sequence (venue-specific source) |
| `seq2` | `uint64` | 8 | End sequence (venue-specific source) |
| `exchangeTimestamp` | `int64` | 8 | Epoch nanoseconds, or -1 if absent |
| `ingressTimestamp` | `int64` | 8 | Gateway receive time, epoch nanoseconds |

**Repeating Group — `BOOK_LEVEL` template (L1 / L2) — 20 bytes per entry**

| Field | Type | Bytes | Notes |
|---|---|---|---|
| `side` | `uint8` | 1 | BID=1, ASK=2 |
| `action` | `uint8` | 1 | UPSERT=1, DELETE=2 |
| `priceScale` | `uint8` | 1 | Decimal scale for priceMantissa |
| `qtyScale` | `uint8` | 1 | Decimal scale for qtyMantissa |
| `priceMantissa` | `int64` | 8 | Signed price mantissa |
| `qtyMantissa` | `int64` | 8 | Signed quantity mantissa |

**Repeating Group — `ORDER_ENTRY` template (L3) — 29 bytes per entry**

| Field | Type | Bytes | Notes |
|---|---|---|---|
| `orderId` | `uint64` | 8 | Exchange-assigned stable order identifier |
| `side` | `uint8` | 1 | BID=1, ASK=2 |
| `action` | `uint8` | 1 | UPSERT=1 (new/modified), DELETE=2 (cancelled/filled) |
| `orderType` | `uint8` | 1 | LIMIT=1, MARKET=2, STOP=3 |
| `priceScale` | `uint8` | 1 | Decimal scale for priceMantissa |
| `qtyScale` | `uint8` | 1 | Decimal scale for qtyMantissa |
| `priceMantissa` | `int64` | 8 | Signed price mantissa |
| `qtyMantissa` | `int64` | 8 | Signed remaining quantity mantissa |

### Encoded Message Size Formulas

```
Header:             8 bytes  (constant)
Body:              47 bytes  (constant, includes bookDepth)

BOOK_LEVEL total:   8 + 47 + (entryCount × 20)
ORDER_ENTRY total:  8 + 47 + (entryCount × 29)
```

Maximum supported entries per message: **10,000**

Maximum encoded sizes:
```
BOOK_LEVEL:   8 + 47 + (10,000 × 20) = 200,055 bytes
ORDER_ENTRY:  8 + 47 + (10,000 × 29) = 290,055 bytes
```

Each `ConnectorFactory` sizes the encoder buffer for its template using the
appropriate formula plus configured headroom bytes. A `COINBASE_L2` connector
uses the `BOOK_LEVEL` formula. A `COINBASE_L3` connector uses the `ORDER_ENTRY`
formula. The encoder is constructed with the right capacity — it never needs
to know about the other template.

### Enum Values

**`eventType`**: BOOK_RESET=1, BOOK_SNAPSHOT=2, BOOK_UPDATE=3

**`venue`**: see Section 23 — each value encodes exchange + depth level as an atomic identity

**`bookDepth`**: L1=1, L2=2, L3=3

**`templateId`**: BOOK_LEVEL=1, ORDER_ENTRY=2

**`side`**: BID=1, ASK=2

**`action`** (BOOK_LEVEL): UPSERT=1, DELETE=2

**`action`** (ORDER_ENTRY): UPSERT=1 (order added or size modified), DELETE=2 (order cancelled or fully filled)

**`orderType`** (ORDER_ENTRY only): LIMIT=1, MARKET=2, STOP=3

### Timestamp Rules

**`exchangeTimestamp`**
- Source: exchange-provided timestamp when present
- Coinbase `snapshot`: use top-level `time` field if present and non-zero;
  encode `-1` if the field is absent, empty, or epoch-zero.
  The Exchange Direct Feed includes `time` on snapshot messages.
  Do not use any per-level timestamp field.
- Coinbase `l2update`: use top-level `time` field
- Parse as UTC; convert to epoch nanoseconds; preserve available precision
- Any absent/unknown sentinel (empty string, null, `0`, epoch-zero text) → encode as `-1`

**`ingressTimestamp`**
- Source: `CachedNanoClock.nanoTime()` captured when the frame enters the pipeline
- Always populated
- **Precision caveat:** `CachedNanoClock` is updated once per Netty event-loop
  iteration. If multiple frames arrive in one iteration, all share the same cached
  timestamp. This means `ingressTimestamp` has *iteration-level* granularity, not
  true per-frame granularity. Under burst conditions this can understate latency
  for all frames in the burst except the first.
  Downstream consumers must treat `ingressTimestamp` as an iteration boundary
  marker, not a precise per-frame arrival time. For true per-frame measurement,
  call `System.nanoTime()` once at the top of `onTextFrame()` and pass the result
  directly to `beginMessage()` — accepting the 30–100ns syscall cost per frame.

### Capacity Limits and Overflow Policy

- Messages exceeding 10,000 levels: **reject** — do not allocate a larger buffer
- Messages exceeding the encode buffer capacity: **reject**
- Rejection must: increment overflow counters, emit structured rate-limited logging,
  trigger recovery when appropriate

### BOOK_RESET Encoding

`BOOK_RESET` is a zero-entry SBE message. It uses the same header + body layout
as any other message, with `entryCount = 0` and no repeating group bytes.

```
Header (8 bytes):
  magic         = 0xEB0B              — protocol marker (little-endian: 0B EB)
  version       = 1                   — current schema version
  templateId    = templateIdByte passed to publishReset()
  blockLength   = 47                  — body length (same constant for all templates)
  entryCount    = 0                   — written directly by the publisher reset encoder

Body (47 bytes):
  eventType     = BOOK_RESET (1)
  venue         = venueByte passed to publishReset()
  bookDepth     = bookDepthByte passed to publishReset()
  instrumentId  = this connector's instrumentId
  gatewayMessageSeq = 0          — BOOK_RESET does not consume a sequence number
  seq1          = 0
  seq2          = 0
  exchangeTimestamp = -1         — no exchange event associated
  ingressTimestamp  = nanoClock.nanoTime() passed to publishReset()

Repeating group: (empty — 0 bytes)
```

Total size: 8 + 47 = 55 bytes.

`publishReset()` is encoded directly by the `Publisher` implementation as a
zero-entry `BOOK_RESET` control message using the passed `venueByte`,
`bookDepthByte`, `templateIdByte`, and `NanoClock` to write the header/body fields and stamp
`ingressTimestamp`. It does not use the connector's reusable `SbeEncoder`
buffer and does not participate in `publisher_latency_*`.
`gatewayMessageSeq` is set to 0 rather than `sequenceTracker().next()` because
a reset marks the end of a session — sequence numbers restart from 1 with the
next `BOOK_SNAPSHOT` after recovery completes.

### Consumers

- Use `blockLength` from the header to locate the repeating group — do not hard-code offsets
- Epoch-zero exchange timestamp (`0` nanoseconds) encodes as `-1`

---

## 18. Low-Latency Implementation Guidelines

The following guidelines apply to every class in the hot-path pipeline.
Violations are bugs, not style issues. Each guideline states the rule,
the reason, and the consequence of violation.

---

### 18.1 Memory and Allocation

**Pre-allocate everything at startup. Allocate nothing on the hot path.**

All buffers, arrays, lookup tables, object pools, and context objects must be
created during `init()` or `buildSessionContexts()`. The hot path — from frame
arrival to `endMessage(publisher, counters, nanoClock)` — must produce zero heap allocations.

Specific rules:
- Use `byte[]` over `String` wherever a stable byte representation suffices.
  `String` allocation on the hot path is a guaranteed GC pressure source.
- Use primitive arrays (`long[]`, `int[]`, `byte[]`) over boxed collections.
  `ArrayList<Long>` allocates a `Long` object per element.
- Use `long[]` result arrays passed into methods rather than returning objects.
  For example, `ByteBufScanner.parseDecimal(buf, result)` writes into a
  pre-allocated `long[2]` rather than returning a new object.
- Pre-allocate all reusable decode/encode scratch state at connector startup.
  Never allocate scratch objects per frame, per level, or per order.
- The encode buffer (`SbeEncoder`) is allocated once per connector in `init()`.
  `endMessage()` reclaims it immediately — no copy, no pool, no reference retained.

**Avoid these on the hot path without exception:**
```
new Object()           — any heap allocation
String.format()        — allocates String + char[]
"a" + "b"             — allocates StringBuilder + String
Arrays.copyOf()        — allocates new array
Collections.sort()     — may allocate temp storage
Stream.of()            — allocates Stream + lambda capture
Optional.of()          — allocates Optional wrapper
```

---

### 18.2 Object Pooling vs Pre-allocation

Do not use object pools on the hot path. Use pre-allocation instead.

Object pools introduce conditional branching (pool-hit vs pool-miss),
require thread-safe acquisition (memory barriers), and still allocate on
pool exhaustion. Pre-allocation has none of these costs — the object is
always present, always on the same thread, always available immediately.

**Correct pattern:** Pre-allocate one reusable object per connector at startup.
The connector owns it exclusively (single-writer). Reset its state before each use.

```java
// CORRECT — pre-allocated scratch, reset per use
private final long[] decimalResult = new long[2]; // mantissa[0], scale[1]

public void onTextFrame(ByteBuf frame, ParseContext ctx) {
    ByteBufScanner.parseDecimal(frame, decimalResult);
    long mantissa = decimalResult[0];
    int  scale    = (int) decimalResult[1];
}

// WRONG — pool
private final ObjectPool<ParseResult> pool = new ObjectPool<>(ParseResult::new);

public void onTextFrame(ByteBuf frame, ParseContext ctx) {
    ParseResult r = pool.acquire();  // branch + barrier
    try { /* ... */ } finally { pool.release(r); }
}
```

---

### 18.3 False Sharing and Cache Line Alignment

**Keep hot-path fields on separate cache lines from fields written by other threads.**

A CPU cache line is 64 bytes. If a counter written by the metrics thread shares
a cache line with a field read by the event-loop thread, every metrics write
invalidates the event-loop thread's cache entry — a hidden latency source.

Rules:
- Separate hot-path connector fields (encoder, parseCtx, clock) from cold-path
  admin fields (shutdown flag, metrics snapshot) using `@Contended` or padding.
- Agrona `AtomicCounter` is already cache-line padded — do not wrap it in another object.
- Do not put multiple `volatile` fields on the same object if they are written
  by different threads. Split them into separate objects with padding.

```java
// WRONG — hotField and coldField share a cache line
public class Connector {
    private volatile boolean shutdownRequested; // written by shutdown thread
    private long             lastSeq;           // read by event-loop thread
}

// CORRECT — padded separation
public class Connector {
    private volatile boolean shutdownRequested;
    private long p1, p2, p3, p4, p5, p6, p7;  // cache line padding
    private long lastSeq;
}
```

---

### 18.4 Branchless and Branch-Predictable Code

**Minimize unpredictable branches on the hot path.**

Modern CPUs speculatively execute branches. A misprediction costs 10–20 cycles.
On a path executing millions of times per second, mispredictions accumulate.

Rules:
- Use straight-line loops over arrays instead of conditionals inside loops.
- Replace `if (x == 'b') side = BID; else side = ASK;` with a lookup table:
  ```java
  // CORRECT — lookup table, no branch
  private static final byte[] SIDE_BY_CHAR = new byte[128];
  static {
      SIDE_BY_CHAR['b'] = Side.BID;
      SIDE_BY_CHAR['s'] = Side.ASK;
  }
  byte side = SIDE_BY_CHAR[firstChar]; // single array load, no branch
  ```
- Use `|=` / `&=` instead of `if/else` to update state flags where possible.
- Order `if` checks with the most common outcome first so the CPU's static
  predictor is correct most of the time.
- The `snapshotGatekeeper.isReady()` check at the top of `onTextFrame` is
  always `true` after the first snapshot — the branch predictor will learn this.
  Keep it as the first check so the misprediction happens only on the first
  frame after each recovery.

---

### 18.5 Mechanical Sympathy — NUMA and CPU Topology

**Keep each connector's data on the same CPU socket as its event-loop thread.**

In multi-socket servers, memory access across NUMA nodes takes 2–3× longer
than local access. If the event-loop thread runs on socket 0 but the encode
buffer was allocated by the main thread on socket 1's memory, every buffer
write crosses the NUMA boundary.

Rules:
- Allocate connector-owned memory (encode buffer, scratch arrays) from within
  the Netty event-loop thread, not from `GatewayBootstrap`. This ensures the
  JVM allocates memory on the local NUMA node for that thread.
- `SbeEncoder` construction in `AbstractConnector.init()` currently happens on
  the bootstrap thread. The connector should defer `ByteBuffer.allocateDirect()`
  to the first execution on the event-loop thread via
  `EpollEventLoop.execute(() -> allocateBuffer())` during `connect()`.
- Pin hot-path threads to cores on the same socket as the NIC receiving
  the market data feed. Use `numactl` or OpenHFT `Java-Thread-Affinity`.
- Verify memory locality with `numastat` after deployment.

---

### 18.6 Lock-Free Algorithms for Shared State

**Never acquire a lock on the hot path. Use lock-free primitives instead.**

Locks introduce unbounded latency — a thread holding a lock can be preempted,
causing the waiting thread to park and context-switch. Even uncontended locks
take tens of nanoseconds due to memory barriers.

Where shared state is unavoidable:
- Use Agrona `AtomicCounter` (backed by `sun.misc.Unsafe` CAS) for counters.
- Use `volatile` reads/writes for single-writer, multiple-reader state
  (e.g. the `recoveryInProgress` flag read by the admin thread).
- Use `ManyToOneConcurrentArrayQueue` or `OneToOneConcurrentArrayQueue`
  from Agrona for off-thread → event-loop handoff (e.g. recovery signals).
- The event-loop thread must never block waiting for a lock. If a lock would
  be needed, the design is wrong — restructure so the event-loop thread is the
  sole writer.

**Single-writer principle:** Every mutable hot-path data structure has exactly
one writer — the owning event-loop thread. Readers on other threads observe via
`volatile` or memory-mapped shared memory. This eliminates all synchronization
costs on the write path.

---

### 18.7 Busy-Wait vs Blocking I/O

**Use busy-wait only when a full CPU core is dedicated and latency matters more than throughput.**

Blocking I/O (`epoll_wait` with a timeout) introduces up to 1ms of kernel
scheduling latency. Busy-wait (spin-loop) eliminates this at the cost of
burning 100% of one CPU core.

Rules:
- Busy-wait is controlled by `transport.busyWaitEnabled` in the config.
  It must never be enabled without also pinning that thread to a dedicated core.
- Use Netty's `EpollEventLoop` with a spin-loop select strategy for latency mode.
- Busy-wait is appropriate for: latency-critical instruments, co-located
  deployments, and benchmarking. It is not appropriate for cloud VMs where
  the hypervisor can steal CPU time unpredictably.
- When busy-wait is disabled, use `EpollEventLoop`'s normal blocking strategy
  for CPU efficiency.

---

### 18.8 Garbage Collection Strategy

**Choose the right GC mode for the deployment. Configure before production.**

No GC is truly zero-latency. The goal is to eliminate allocation on the hot
path so GC runs are rare and short.

Rules:
- **Preferred collector: ZGC (Java 21)** — sub-millisecond pause times,
  concurrent, works well with large direct-memory workloads:
  ```
  -XX:+UseZGC -XX:+ZGenerational -XX:+ExplicitGCInvokesConcurrent
  ```
  `ExplicitGCInvokesConcurrent` ensures that `System.gc()` (called during
  warmup) triggers ZGC's concurrent cycle rather than a stop-the-world fallback.
- **Alternative: Shenandoah** — concurrent, similar pause characteristics to ZGC.
- **Avoid: G1GC** for latency-critical paths — mixed-collection pauses can
  reach tens of milliseconds under heap pressure.
- **Avoid: Parallel GC** — stop-the-world, unacceptable for deterministic latency.
- Fix heap size to avoid GC-triggered growth:
  ```
  -Xms4g -Xmx4g
  -XX:MaxDirectMemorySize=2g
  ```
- Run a warm-up period before the feed goes live to force JIT compilation and
  GC region initialization.
- Use JFR (`-XX:StartFlightRecording=settings=profile`) to observe GC and
  allocation events alongside latency percentiles during load testing.

---

### 18.9 JIT Compilation and Method Inlining

**Structure hot-path code to be JIT-friendly.**

The JVM JIT inlines methods up to a default size threshold (~35 bytecodes).
Methods that exceed this are not inlined, adding a method-dispatch cost.

Rules:
- Keep `onTextFrame()` parsing loops short. Extract sub-parsing into small
  private methods that the JIT can inline.
- Avoid polymorphic dispatch on the hot path. `FeedParser` is an interface, but
  the JIT will monomorphize the call site after observing only one concrete type —
  one connector always calls one parser. Keep it that way.
- Mark hot-path utility methods in `ByteBufScanner` as small and self-contained
  so the JIT inlines them automatically.
- Use `-XX:+PrintInlining` during profiling to verify critical methods inline.
- Avoid `try/catch` blocks inside tight parsing loops — exception handling
  prevents inlining and disables some JIT optimizations in surrounding code.
  Validate inputs explicitly; reserve exceptions for truly exceptional conditions.

---

### 18.10 Measuring Latency Correctly

**Use the right tool. Wall-clock benchmarks lie.**

Common pitfalls:
- **Coordinated omission:** if your benchmark sends the next message only after
  the previous one completes, slow messages hide behind a queue. Use a
  constant-rate sender to observe tail latency accurately.
- **JVM warm-up:** measure only after 100,000+ iterations.
- **`System.nanoTime()` on the hot path:** calling `nanoTime()` directly can
  invoke a system call on some Linux configurations, adding 30–100ns per call.
  Use Agrona `CachedNanoClock` — updated once per event-loop iteration — to
  amortize this cost across all messages in that iteration.
- **Allocation-induced latency:** use JFR to observe GC and allocation events
  alongside latency percentiles.

Measurement approach for this gateway:
```
1. Capture ingressTimestamp at frame arrival using CachedNanoClock
2. Capture handoffTimestamp on the connector thread immediately before
   calling `publisher.publish()`
3. Compute connector handoff latency = handoffTimestamp - ingressTimestamp
4. Track connector handoff latency via `gateway_handoff_latency_*` counters
5. Separately, every publisher must measure publisher-stage latency from entry
   to return of `publish()` using the caller-provided per-connector `NanoClock`
   passed into `publish()`, and report it via `publisher_latency_*` counters
6. Known limitation in this revision: because the passed `CachedNanoClock` is
   refreshed once per event-loop iteration, publisher latency may remain zero
   or otherwise quantized for publishes that complete within the same iteration
7. Publisher latency measures publisher-stage cost only and must NOT be mixed
   with connector handoff latency
8. After Phase 3 is stable: add HdrHistogram for p50/p99/p99.9 percentiles
```

---

### 18.11 Off-Heap Memory Layout and Alignment

**Align fields to their natural size boundaries to avoid unaligned access penalties.**

On x86/x64, unaligned reads carry a penalty when crossing a cache-line boundary.
On ARM (common in cloud deployments), unaligned reads may trap to a slow path.

For the SBE repeating group (`BOOK_LEVEL`), the current layout places 4 byte-sized
fields before the first `int64`. This means `priceMantissa` is at a non-8-byte-
aligned offset within each entry. Consider reordering the repeating group fields
to place the `int64` fields first:

```
Reordered BOOK_LEVEL repeating group (still 20 bytes):
  priceMantissa  int64  8 bytes  — 8-byte aligned at entry start
  qtyMantissa    int64  8 bytes  — 8-byte aligned
  side           uint8  1 byte
  action         uint8  1 byte
  priceScale     uint8  1 byte
  qtyScale       uint8  1 byte
```

Verify the layout is alignment-correct with compile-time assertion tests
in `SbeEncoderTest`.

---

### 18.12 Deterministic Startup and Warmup Sequence

**Never let the first live message be the worst-latency message.**

Required warmup sequence before connecting to the live feed:
```
1. GatewayBootstrap.init() completes — all objects allocated
2. Run 100,000 synthetic frames through the full hot path
   (parse → encode → in-memory publish) using pre-recorded fixtures
3. Wait for JIT to reach C2 tier (observable via JFR)
4. Trigger a concurrent GC cycle to clear warmup garbage:
     System.gc();           // hint only — requires -XX:+ExplicitGCInvokesConcurrent
     Thread.sleep(500);     // allow ZGC's concurrent cycle to complete
   Note: with ZGC, System.gc() triggers a concurrent cycle (not stop-the-world)
   only when -XX:+ExplicitGCInvokesConcurrent is set. Without this flag, ZGC
   may fall back to a stop-the-world collection. Add to JVM flags:
     -XX:+ExplicitGCInvokesConcurrent
5. Only then: call connector.connect() to establish the live feed
```

The synthetic warmup must exercise all code paths:
- Snapshot parsing (opens the `SnapshotGatekeeper`)
- Update parsing (full encoding + publishing cycle)
- Heartbeat processing (counter increment path)
- Malformed frame rejection (error path)

---

### 18.13 Instrument Lookup — Linear Scan vs Hash Table

**Symbol resolution must be O(1) with no allocation.**

**For small sets (≤ 32 instruments) — byte-array linear scan:**
```java
private final byte[][] symbolBytes;   // exchangeSymbol as byte arrays, pre-built
private final int[]    instrumentIds; // corresponding instrumentIds
private final int      count;

public int resolveInstrumentId(ByteBuf buf, int offset, int length) {
    for (int i = 0; i < count; i++) {
        if (ByteBufScanner.bytesEqual(buf, offset, length, symbolBytes[i])) {
            return instrumentIds[i];
        }
    }
    return -1; // not found — caller increments unknownSymbolDrops counter
}
```
At 32 instruments × ~8 bytes per comparison, this fits in one or two cache
line groups. No hashing, no allocation, branch-predictor-friendly.

**For larger sets — open-addressing hash table with byte-array keys:**
- Key: `exchangeSymbol` as `byte[]`. Value: `int instrumentId`.
- Use FNV-1a hash on raw bytes — fast, allocation-free, good distribution.
- Load factor ≤ 0.5 to minimize collision chains.
- Never resize after startup.

**Never use `HashMap<String, Integer>`** — requires `String` allocation from
the `ByteBuf` bytes before every lookup, plus `Integer` boxing on hit.

---

### 18.14 Rate-Limited Logging and Error Recording

**Logging on the hot path is a latency cliff. Use Agrona `DistinctErrorLog` instead.**

`SLF4J` / `Log4j` direct calls on the parser/connector hot path: string formatting allocates `StringBuilder`
and `String`, synchronous appenders can acquire locks, and I/O writes can block for
hundreds of microseconds.

Rules:
- The parser, connector, and encoder hot path must never call a logging framework directly.
- `LoggingPublisher` is the explicit integration-validation exception: it may
  enqueue copied payload records through Log4j 2 async loggers, but venue code
  must still interact only through the `Publisher` interface.
- Use Agrona `DistinctErrorLog` for errors that may repeat at high frequency
  (malformed frames, parse failures). It deduplicates by error identity and
  writes to a memory-mapped buffer with no lock and minimal allocation.
- For lifecycle events (connect, subscribe, recovery), use structured logging
  on the control path — these are not latency-sensitive.
- Rate-limit repeated structured log entries using a counter gate:
  ```java
  private long lastLoggedAt = 0;
  long now = epochClock.time();
  if (now - lastLoggedAt > RATE_LIMIT_MS) {
      logger.warn("...");
      lastLoggedAt = now;
  }
  counters.parseFailures().increment(); // always increment even when log suppressed
  ```

---

### 18.15 Network Stack and Kernel Bypass Considerations

**Kernel-bypass networking can eliminate the largest remaining latency source.**

After all hot-path optimizations, the dominant latency source shifts to the
kernel network stack.

**Busy-poll with `SO_BUSY_POLL` (Linux):**
- Instructs the kernel to spin on the NIC receive queue rather than sleeping.
- Reduces median receive latency by 20–50μs on dedicated cores.
- Enable via `EpollChannelOption.SO_BUSY_POLL` in Netty's epoll transport.
- Effective only with CPU pinning and a dedicated core.

**Kernel bypass (DPDK / OpenOnload):**
- Bypasses the kernel entirely; the NIC DMA's frames directly into user-space.
- Reduces receive latency to 1–5μs vs 20–100μs with kernel networking.
- Not applicable to the current architecture — the Coinbase Exchange Direct
  Feed is delivered over TLS WebSocket, which requires the kernel TLS stack.
- Relevant if a future venue supports a UDP-based feed (e.g. a co-location
  FIX/FAST feed). Document as a future path in the relevant venue README.

**Current achievable latency targets** for this gateway architecture
(epoll + BoringSSL + off-heap hot path, no kernel bypass):
- Frame arrival to `publisher.publish()`: **< 5μs** at p99 on a co-located
  Linux server with CPU pinning.
- Frame arrival to `publisher.publish()`: **< 50μs** at p99 in a cloud VM
  without CPU pinning.

---

## 19. Staff-Level Recommendations

### Netty

- Use `PooledByteBufAllocator` with direct buffers for transport and frame handling
- Keep the Netty pipeline minimal — no unnecessary handlers, copies, or transformations
- Configure explicit WebSocket/frame size bounds per venue
- Prefer one hot-path event-loop thread per connector

### Transport and I/O

- On Linux: prefer **`EpollEventLoopGroup`** and epoll channel classes from
  `netty-transport-native-epoll` — avoids Java NIO selector overhead
- Use **`netty-tcnative`** with BoringSSL via `SslProvider.OPENSSL` — the inbound
  WebSocket path is TLS-heavy and direct-buffer behavior matters materially
- When native epoll cannot be loaded, including non-Linux development/test
  environments, fall back to `NioEventLoopGroup` while preserving the same
  transport contract.
- For latency-critical instruments: support optional busy-wait / spin-loop select
  strategy — config-driven, enabled only when a full CPU core is reserved and pinned
- **`io_uring` migration path:** do not instantiate `EpollEventLoopGroup` directly
  inside `NettyWebSocketTransport`. Accept the event loop group as a constructor
  parameter supplied by `GatewayBootstrap`. When Netty's `io_uring` support
  matures, swapping `EpollEventLoopGroup` for `IoUringEventLoopGroup` in
  `GatewayBootstrap` becomes a one-line change with zero impact on transport code.
- Consider enabling `SO_BUSY_POLL` via `EpollChannelOption` when CPU pinning
  is active — reduces median receive latency by 20–50μs

### Instruments Per Instance

- **Recommended maximum: 20–30 instruments per instance** for CPU-pinned
  co-located deployments. Each instrument requires one hot-path event-loop thread,
  one TLS session, one WebSocket connection, and one encode buffer. At 50+
  instruments the thread count, memory footprint, and OS scheduling pressure
  become significant. For large instrument sets, deploy multiple gateway instances
  (each with a disjoint instrument list) behind a shared downstream consumer.
- Document the target instrument count in the deployment runbook. Monitor
  `activeConnections` and per-thread CPU utilization as the primary signals.

### TLS Session Resumption During Recovery

- A full TLS handshake costs 1–3 RTTs (3–15ms at co-location speeds). This is
  the dominant latency cost during recovery — longer than reconnect, subscribe,
  or snapshot acquisition in most cases.
- Enable TLS session resumption to reduce reconnect-path TLS cost to 1 RTT:
  ```java
  SslContextBuilder.forClient()
      .sslProvider(SslProvider.OPENSSL)
      .sessionCacheSize(64)         // cache up to 64 sessions
      .sessionTimeout(3600)         // 1 hour — longer than any planned outage
      .build();
  ```
- BoringSSL supports both session IDs and session tickets. Session tickets are
  preferred — they are stateless on the server and survive server-side restarts.
  The Coinbase Exchange server must support session tickets for this to work;
  verify during integration testing.
- If session resumption is unavailable (session expired, server does not support
  tickets, IP change), the full handshake proceeds transparently — no special
  handling needed. Resumption is an optimization, not a correctness requirement.

### CPU Affinity

- Pin only hot-path connector/event-loop threads
- Config-driven and optional — keep thread-to-core placement stable for cache locality
- Use OpenHFT `Java-Thread-Affinity` (`net.openhft:affinity`, `AffinityLock`) where supported
- Acquire affinity lock from within the Netty event-loop thread during its initialization
- `transport.cpuAffinity.enabled: false` disables affinity and performs no pinning
- `transport.cpuAffinity.mode: AUTO` lets OpenHFT acquire an available suitable lock for each connector event-loop thread
- `transport.cpuAffinity.mode: EXPLICIT` pins connector event-loop threads to `transport.cpuAffinity.eventLoopCpuIds` in connector/instrument order
- `transport.cpuAffinity.eventLoopCpuIds` must be non-empty when mode is `EXPLICIT`; CPU ids must be non-negative and must not contain duplicates
- Unsupported OpenHFT/platform affinity should log a diagnostic and continue when `busyWaitEnabled` is false
- If `busyWaitEnabled` is true and affinity cannot be acquired, startup must fail because busy waiting without stable CPU pinning violates the latency model
- Pin threads to cores on the same NUMA node as the NIC — verify with `numastat`

### GC and JVM

- **Preferred: ZGC with `+ZGenerational`** (Java 21) — sub-millisecond pauses
- Fix heap size: `-Xms=Xmx` — eliminates heap-growth pauses
- Pre-size direct memory: `-XX:MaxDirectMemorySize` — headroom for Netty + Agrona
- Warm up with synthetic frames before connecting to the live feed
- Monitor GC via JFR; treat any pause > 1ms as a regression

### Symbol Resolution

- Precompute `exchangeSymbol` → `instrumentId` mapping at startup
- For ≤ 32 instruments: byte-array linear scan (fits in cache, zero allocation)
- For > 32 instruments: open-addressing hash table with FNV-1a on raw bytes
- Never use `HashMap<String, Integer>` — requires String allocation per lookup

### Timestamp Parsing

- Treat timestamp parsing as a hot-path cost — benchmark early
- Implement a specialized ASCII parser for RFC 3339 timestamps that:
  - Converts year/month/day to days-since-epoch
  - Handles month lengths and leap-year logic
  - Parses hour/minute/second/fraction with variable precision
  - Scales fractions to nanoseconds
  - Detects epoch-zero sentinels and maps them to `-1`
- Never allocate `Instant`, `OffsetDateTime`, or `DateTimeFormatter` on the hot path
- Benchmark timestamp parsing as part of hot-path performance validation

### Publisher Guarantees

- Synchronous publishers read or forward all data within `publish()` and return
- Asynchronous publishers copy bytes into a pre-allocated off-heap ring or pool before returning
- If publisher cannot accept after bounded retries: drop, increment counters, trigger recovery
- The gateway must not block or spin indefinitely on the event-loop thread waiting for publisher capacity

---

## 20. Publisher Model

### Contract

Publisher interfaces are designed around reusable direct-buffer handoff on a
shared publisher instance:

```java
boolean publish(DirectBuffer buffer, int offset, int length,
                InstrumentCounters counters,
                NanoClock nanoClock);
void    publishReset(int instrumentId, byte venueByte,
                     byte bookDepthByte, byte templateIdByte,
                     NanoClock nanoClock);
```

**Latency naming rule:**
- `handoffTimestamp`: captured on the connector thread immediately before
  `publisher.publish()`
- `gateway_handoff_latency_*`: ingress-to-handoff timing owned by the connector
- `publisher_latency_*`: publisher-stage timing owned by the publisher; never
  reuse `gateway_handoff_latency_*` for this

Required semantics:
- `publisher_latency_*` is mandatory for all publisher implementations
- Timing starts on entry to `publish()` and ends when `publish()` returns,
  using the caller-provided per-connector `NanoClock`
- For async publishers, this is enqueue latency only; downstream completion
  latency is a separate concern and must not be reported here
- `publishReset()` is explicitly excluded from `publisher_latency_*`; reset
  publication during recovery/shutdown must not modify publisher latency stats,
  even though it receives `venueByte`, `bookDepthByte`, `templateIdByte`, and `NanoClock` for
  reset-message encoding and timestamping
- The shared publisher receives `InstrumentCounters` and the caller's `NanoClock`
  on every `publish(...)` call
- Known limitation in this revision: because `CachedNanoClock` is refreshed once
  per event-loop iteration, `publisher_latency_*` may remain zero or otherwise
  quantized for publishes completed within one iteration. This behavior is
  accepted for now; the method contract is fixed so a finer-grained timing
  policy can be introduced later without another API change
- Once a publisher implementation has a `latencyNanos` value, it must update
  counters via the shared `PublisherLatencyStats.record(...)` helper below
- `publisherLatencyMinNanos()` uses `0` as the initial counter state. The helper
  must treat `currentMin == 0` as "first observation" and replace it with the
  measured latency; afterward it uses `min(currentMin, latencyNanos)`. Because
  this revision may legitimately observe `0`, the recorded min may remain `0`
  once such a value is seen

```java
// core/publisher/PublisherLatencyStats.java
public final class PublisherLatencyStats {
    private PublisherLatencyStats() {}

    public static void record(InstrumentCounters counters, long latencyNanos) {
        counters.publisherLatencyLastNanos().set(latencyNanos);
        long currentMin = counters.publisherLatencyMinNanos().get();
        long nextMin = (currentMin == 0L)
            ? latencyNanos
            : Math.min(currentMin, latencyNanos);
        counters.publisherLatencyMinNanos().setRelease(nextMin);
        counters.publisherLatencyMaxNanos().setRelease(
            Math.max(counters.publisherLatencyMaxNanos().get(), latencyNanos));
    }
}
```

### Publisher Selection — `resolvePublisher()`

`GatewayBootstrap.resolvePublisher(GatewayConfig config)` instantiates the
correct `Publisher` based on `config.publisher.type`. This is the only place
publisher selection logic lives.

```java
private static Publisher resolvePublisher(GatewayConfig config) {
    PublisherConfig pc = config.publisher;
    if (pc == null || pc.type == null || pc.type.isBlank()) {
        throw new IllegalArgumentException(
            "publisher.type is required in gateway.yaml. " +
            "Valid values: LOGGING, IN_MEMORY, SHARED_MEMORY, AERON, CHRONICLE");
    }
    // Parse the String → PublisherType here so that invalid enum text produces
    // a controlled IllegalArgumentException rather than a SnakeYAML YAMLException
    // from binding time. This gives GatewayBootstrap full ownership of the message.
    PublisherType type;
    try {
        type = PublisherType.valueOf(pc.type.toUpperCase());
    } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unknown publisher.type '" + pc.type + "'. " +
            "Valid values: LOGGING, IN_MEMORY, SHARED_MEMORY, AERON, CHRONICLE");
    }
    return switch (type) {
        case LOGGING    -> new LoggingPublisher(pc.logging);
        case IN_MEMORY  -> new InMemoryPublisher();
        case SHARED_MEMORY, AERON, CHRONICLE ->
            // Not yet implemented. Fail startup with a clear message rather than
            // starting up with a broken or no-op publisher.
            throw new IllegalArgumentException(
                "publisher.type=" + type + " is not implemented in this build. " +
                "Use LOGGING for integration validation.");
    };
}
```

**Startup failure rules for publisher configuration — explicit and exhaustive:**

| Condition | Failure point | Result |
|---|---|---|
| `publisher` block missing from YAML | `resolvePublisher()` | `IllegalArgumentException` — publisher.type required |
| `publisher.type` missing or blank | `resolvePublisher()` | `IllegalArgumentException` — publisher.type required |
| `publisher.type` is an unrecognised string | `resolvePublisher()` | `IllegalArgumentException` — names valid values |
| `publisher.type = SHARED_MEMORY` | `resolvePublisher()` | `IllegalArgumentException` — not implemented |
| `publisher.type = AERON` | `resolvePublisher()` | `IllegalArgumentException` — not implemented |
| `publisher.type = CHRONICLE` | `resolvePublisher()` | `IllegalArgumentException` — not implemented |
| `publisher.type = LOGGING`, `logging` block missing | `resolvePublisher()` | `IllegalArgumentException` — outputPath required |
| `publisher.type = IN_MEMORY` | — | Succeeds — valid for tests only |
| `publisher.type = LOGGING` with valid config | — | Succeeds — integration validation adapter |

Because `PublisherConfig.type` is declared as `String` (not `PublisherType`), SnakeYAML
never attempts to bind the YAML value to an enum. All validation happens inside
`resolvePublisher()` after binding, ensuring every failure is an `IllegalArgumentException`
with a message controlled by this spec. `GatewayBootstrap` catches all
`IllegalArgumentException` at startup, logs the message, and exits the process
before any connector is initialized or any network connection is attempted.

### `LoggingPublisher` - Log4j 2 Async Integration Validation Adapter

```java
// core/publisher/LoggingPublisher.java
/**
 * Publisher that writes encoded SBE messages through a dedicated Log4j 2 async
 * logger context backed by the LMAX Disruptor.
 *
 * PURPOSE: Integration validation only. Use this adapter to verify the full
 * gateway pipeline - parse -> encode -> publish - is working correctly before
 * connecting a real downstream (Aeron, Chronicle Queue, shared memory).
 *
 * IMPLEMENTATION NOTE - async Log4j 2 write path:
 * publish() must not write to disk directly on the event-loop thread. It must
 * copy the requested DirectBuffer region before returning, encode the copied
 * bytes into an immutable log payload, and hand that payload to a Log4j 2
 * AsyncLoggerContext. The async logger context uses the LMAX Disruptor to move
 * log events to the rolling file appender thread.
 *
 * Log format: one encoded SBE payload per line:
 *   sbe_base64=<base64 encoded encoded gateway message bytes>
 *
 * Rolling file output is configured by LoggingPublisherConfig:
 *   outputPath - active message log file
 *   rollSizeMb - maximum active log size before rolling
 *
 * This adapter is intentionally not part of the zero-allocation hot-path
 * acceptance criteria. It is an integration-validation sink that trades
 * allocation for operational simplicity and a durable local trace.
 *
 * Thread-compatible: publish() may be called from connector event-loop threads;
 * the publisher must not retain the DirectBuffer reference after returning.
 */
public final class LoggingPublisher implements Publisher, AutoCloseable {

    public LoggingPublisher(LoggingPublisherConfig config) { ... }

    @Override
    public boolean publish(DirectBuffer buffer, int offset, int length,
                           InstrumentCounters counters,
                           NanoClock nanoClock) {
        // Validate buffer region.
        // Copy [offset, offset + length) before returning.
        // Base64-encode the copied bytes and call asyncLogger.info(...).
        // Increment messagesPublished and update publisher_latency_* with
        // enqueue/copy latency only, not downstream file flush latency.
        return true;
    }

    @Override
    public void publishReset(int instrumentId, byte venueByte,
                             byte bookDepthByte, byte templateIdByte,
                             NanoClock nanoClock) {
        // Encode a zero-entry BOOK_RESET prefix and log it as sbe_base64=...
        // Do not update publisher_latency_* for reset control messages.
    }

    @Override
    public void close() {
        // Flush queued async Log4j events and shut down the dedicated context.
    }
}
```

`LoggingPublisher` adds to the project structure:
```
core/publisher/
  Publisher.java
  InMemoryPublisher.java
  LoggingPublisher.java        <- Log4j 2 async integration validation adapter
```

### Ownership

The encode buffer is a single reusable off-heap allocation owned by the encoder.
The buffer ownership contract is:

**The publisher must not retain the `DirectBuffer` reference after `publish()` returns.**

The encoder reclaims the buffer for the next `beginMessage()` call as soon as
`publish()` returns. The precise ownership model per publisher type:

| Publisher type | What it must do before publish() returns | May store buffer ref? |
|---|---|---|
| Synchronous | Read or forward all data within the call | **No** |
| Async (Aeron, Chronicle, Kafka adapter) | Copy `[offset, offset+length)` into its own pre-allocated off-heap slot | **No** — store the copy, not the ref |

The copy for async publishers must complete synchronously before `publish()`
returns. Scheduling the copy as a future task after returning is a data
corruption bug — the encoder will overwrite the buffer before the task runs.

For async publishers, the off-heap copy destination must be:
- Pre-allocated (no per-message allocation)
- Bounded (fixed-size ring or pool — finite number of slots)
- Off-heap (Agrona `UnsafeBuffer` over direct `ByteBuffer`)

If all slots are occupied when `publish()` is called, the publisher must fail
fast after a bounded number of non-blocking offer attempts, return `false`,
and allow the encoder to trigger recovery. It must not block the event-loop thread.

No hidden copy is permitted on the synchronous path unless the downstream
transport requires it.

### Downstream Recovery Signaling

External downstream systems may request recovery via explicit request messages.

**Minimum request fields:**

| Field | Type | Purpose |
|---|---|---|
| `venue` | `VenueEnum` | Identifies the target gateway instance. Requests with a venue that does not match the configured instance venue are silently ignored. |
| `instrumentId` | `int` | Identifies the instrument within the venue. |
| `requestType` | `RecoveryRequestType` | What kind of recovery is being requested. |
| `reasonCode` | `RecoveryReasonCode` | Why recovery is being requested. |
| `requestTimestamp` | `long` | Epoch nanoseconds when the request was generated. |

```java
// core/recovery/RecoveryRequest.java
public final class RecoveryRequest {
    public final VenueEnum           venue;           // must match configured instance venue
    public final int                 instrumentId;
    public final RecoveryRequestType requestType;
    public final RecoveryReasonCode  reasonCode;
    public final long                requestTimestamp; // epoch nanoseconds
    public final String              diagnosticText;  // optional; null on hot path

    public RecoveryRequest(VenueEnum venue, int instrumentId,
                           RecoveryRequestType requestType,
                           RecoveryReasonCode reasonCode,
                           long requestTimestamp) { ... }
}
```

**Example adapter-facing call:**
```java
requestRecovery(venue, instrumentId, requestType, reasonCode, requestTimestamp)
```

`requestType` values and their exact behavioral semantics:

| Value | Meaning | Gateway action |
|---|---|---|
| `RESET` | Downstream has lost its local book state and cannot recover it. Requests the gateway to publish a fresh `BOOK_RESET` followed by a fresh `BOOK_SNAPSHOT`. | Treat as a full recovery: publish `BOOK_RESET`, execute the venue recovery strategy (disconnect → reconnect → resubscribe → await snapshot), publish `BOOK_SNAPSHOT`, resume `BOOK_UPDATE`. |
| `RESNAPSHOT` | Downstream has a valid book but needs a checkpoint. Requests the gateway to publish a fresh `BOOK_SNAPSHOT` at the current book state without interrupting the feed if possible. | If the venue supports explicit snapshot requests on an existing connection, request one and publish it as `BOOK_SNAPSHOT`. If not (e.g. Coinbase L2), treat as `RESET` and perform a full reconnect. |
| `RESYNC` | Downstream detected an inconsistency (e.g. failed checksum, unexpected gap). Requests the gateway to validate its own stream state and recover if necessary. | If the gateway's own stream is healthy, the gateway may publish the current `BOOK_SNAPSHOT` as a re-sync point without reconnecting. If the gateway detects its own stream is also inconsistent, treat as `RESET`. |

For the initial `COINBASE_L2` implementation, all three request types execute
the same full recovery path (disconnect → reconnect → resubscribe) because
Coinbase L2 does not support snapshot-on-demand over an existing connection.
`RESNAPSHOT` and `RESYNC` are distinguished in the interface so future venues
with REST snapshot support can implement lighter-weight responses.

`reasonCode` values: `STREAM_INTEGRITY_FAILURE`, `OUT_OF_ORDER_OR_INVALID_TRANSITION`,
`STATE_CORRUPTION`, `CHECK_FAILED`, `HEARTBEAT_TIMEOUT`, `MANUAL_RESET`

The `reasonCode` is informational — the gateway logs it but does not change its
recovery behavior based on it. The `requestType` drives the action.

These requests are additive to the gateway's own autonomous stream-integrity checks.
Recovery signaling must be decoupled from market-data buffers.

---

## 21. Recovery Model

### 21.1 Recovery Semantics

- The gateway autonomously initiates recovery when it detects any validated
  stream-integrity failure for an owned instrument
- The gateway also autonomously initiates recovery when the heartbeat timeout elapses
- Downstream recovery requests are additive — the gateway owns the recovery policy
- Recovery handling is scoped to the affected instrument
- The gateway does not require downstream systems to retain or return buffers
- Recovery state is tracked per `instrumentId`

### 21.2 Recovery Routing

Downstream recovery requests are matched at the gateway boundary as follows:

1. **Venue check**: compare `request.venue` against the single configured instance
   venue. If they do not match, silently ignore the request — it belongs to a
   different gateway instance. Do not log a warning for every mismatch; these
   are expected in broadcast recovery transports where all instances see all requests.
2. **Instrument check**: look up the connector by `request.instrumentId`.
   If no connector owns that `instrumentId`, ignore the request.
3. **In-progress check**: if recovery is already in progress for that `instrumentId`,
   log once (rate-limited) and ignore.
4. **Route**: schedule recovery execution on the matching connector's event-loop thread.

`RecoveryRequest.venue` is a required field (see Section 20, Downstream Recovery
Signaling). A request without a valid venue cannot satisfy the venue check and
must not be acted upon.

Within a single-venue instance, runtime routing is by `instrumentId` only. The
venue check is a boundary filter that happens once per request, not a per-message
dispatch decision.

### 21.3 Recovery State Coalescing

Recovery spans two phases. The `recoveryInProgress` flag covers both:

- **Phase A — Channel work**: `AbstractConnector.recover()` sets `recoveryInProgress = true`
  and delegates to `RecoveryStrategy.execute()`. The strategy disconnects, reconnects,
  and resubscribes, then calls `RecoveryContext.onChannelRestored()`. The connector
  resets session state and triggers snapshot acquisition. The flag remains `true`.

- **Phase B — Snapshot acquisition**: the snapshot strategy fetches or awaits the
  fresh snapshot, publishes `BOOK_SNAPSHOT`, opens the `SnapshotGatekeeper`, then
  calls `SnapshotContext.onSnapshotBoundaryAccepted()`. The connector sets
  `recoveryInProgress = false` and increments `recoveryCompletions`.

While `recoveryInProgress` is `true`:
- Duplicate recovery requests for the same instrument are counted and ignored
- No duplicate `BOOK_RESET`, reconnects, or resubscribes are triggered
- `recoveryCompletions` counter is not incremented until Phase B completes

### 21.4 Venue Recovery Strategy

- Each venue's `RecoveryStrategy` owns the concrete recovery mechanism
- Must execute on the owning event-loop thread
- May use: full disconnect + reconnect + resubscribe, channel resubscribe on
  existing connection, explicit REST snapshot request, or hybrid
- Must signal exactly once: `onChannelRestored()` or `onRecoveryFailed()`
- The downstream recovery interface remains venue-agnostic regardless of venue
  recovery differences

### 21.5 Recovery Ordering Contract

Every recovery — whether self-triggered or downstream-triggered — must follow
this exact ordering:

```
1. Publish BOOK_RESET for the affected instrument
   → recoveryInProgress = true

2. Execute RecoveryStrategy (disconnect → reconnect → resubscribe)
   → RecoveryContext.onChannelRestored() signals channel work done

3. Reset session state, trigger snapshot acquisition
   → recoveryInProgress remains true through this phase

4. Await and accept the fresh snapshot boundary
   → Publish BOOK_SNAPSHOT
   → SnapshotContext.onSnapshotBoundaryAccepted() fires
   → recoveryInProgress = false, recoveryCompletions counter incremented

5. Resume BOOK_UPDATE publishing
```

The `recoveryInProgress` flag spans steps 1 through 4 inclusive. Any recovery
request that arrives while the flag is set is counted and silently ignored —
duplicate `BOOK_RESET`, reconnects, and resubscribes must not occur.

The gateway drops any pre-boundary incremental updates that arrive during steps
2 and 3. These are handled by the `SnapshotGatekeeper` remaining in WAITING state.

### 21.6 Recovery Event Semantics

- `BOOK_RESET`: downstream must discard current state for this instrument
- `BOOK_SNAPSHOT`: downstream must treat as fresh authoritative book image
- `BOOK_UPDATE`: downstream applies incrementally — only valid after BOOK_SNAPSHOT

### 21.7 Graceful Shutdown

On `SIGTERM`:
- Stop accepting new downstream recovery work and new market-data processing
- For each active connector: send authenticated unsubscribe, publish best-effort
  BOOK_RESET, close the WebSocket connection
- For each retained connector-owned `EventLoopGroup`: call
  `shutdownGracefully(quietPeriodMs, timeoutMs, MILLISECONDS)` and await
  termination up to the configured shutdown deadline
- Ensure final counter and error-log state is visible via the mapped buffers
- If shutdown cannot complete within `transport.shutdownDeadlineMs`: stop
  waiting for remaining channels/groups and exit without waiting indefinitely

### 21.8 CoinbaseL2RecoveryStrategy — Initial Implementation

```
1. Send best-effort authenticated unsubscribe for level2 and heartbeat channels
2. Close WebSocket connection
3. Reconnect with exponential backoff + jitter
4. Regenerate Coinbase authentication payload
5. Send authenticated subscribe for level2 and heartbeat
6. Await fresh snapshot boundary (SUBSCRIBE_DRIVEN — arrives automatically)
```

This same path is used for all recovery triggers: sequence gaps, stream-integrity
failures, heartbeat timeout, publisher backpressure.

`CoinbaseL3RecoveryStrategy` uses the same disconnect/reconnect flow but
subscribes to the `full` channel and uses `REST_THEN_DELTA` snapshot acquisition
(REST fetch + buffer replay) rather than awaiting a WebSocket-delivered snapshot.

Both must conform to the generic `RecoveryStrategy` interface so the connector
pattern is identical regardless of depth level.

### 21.9 Scope Boundary

This project implements the market data gateway and its downstream integration
interfaces. It does **not** implement Aeron Cluster, Kafka, Chronicle Queue,
or external consumer logic. Adapter contracts are designed so those systems
can be attached later without changing the hot-path core.

---

## 22. How to Add a New Venue Strategy

A **venue strategy** is the combination of an exchange and a data level —
for example `COINBASE_L2`, `COINBASE_L3`, or `BINANCE_L2`. Each is a fully
independent pluggable strategy. Adding one requires exactly these steps.
**No changes to any existing `core/` class are required.**

The base classes in Section 8 eliminate all boilerplate. A developer writing
a new venue strategy focuses entirely on the three classes that need genuine
exchange-specific logic: `FeedParser`, `SnapshotStrategy`, and `SubscriptionBuilder`.

### Steps

1. **Create `venue/<exchange>/l<n>/` package.**
   Example: `venue/binance/l2/` for `BINANCE_L2`.

2. **Create `venue/<exchange>/shared/` package** (if not already present)
   for auth logic, endpoint constants, and config shared across depth levels.
   Omit if only one depth level will ever be supported.

3. **Extend `AbstractConnector`** — declare three methods only:
   `venueEnum()`, `maxEntryCount()`, `headroomBytes()`. No other code needed.

4. **Extend `AbstractConnectorFactory`** — implement `parseVenueConfig()` and
   `createConnector()`. The factory constructs the venue components and passes
   them to the connector constructor. `SbeEncoder` construction is handled by
   `AbstractConnector.init()` using `venueEnum()` — the factory does not
   instantiate the encoder directly.

5. **Implement `FeedParser`** — the only class requiring substantial code.
   Handwritten `ByteBuf` scanner using `ByteBufScanner` utilities; stateless;
   calls `ctx.encoder().writeLevel()` (BOOK_LEVEL) or `ctx.encoder().writeOrder()`
   (ORDER_ENTRY); applies correct sequence mapping for this venue.

6. **Implement `SubscriptionBuilder`** — builds subscribe/unsubscribe payloads
   for the specific channel at this depth level. Uses auth from `shared/`.

7. **For `SUBSCRIBE_DRIVEN` venues — extend `SubscribeDrivenSnapshotStrategy`.**
   The entire class is one line. The parser handles snapshot recognition.

   **For `REST_THEN_DELTA` venues — implement `SnapshotStrategy` directly.**
   Implement the REST fetch + buffer + sequence alignment algorithm in
   `triggerSnapshot()`. This is the second class requiring substantial code.

8. **For reconnect-style recovery — extend `ReconnectRecoveryStrategy`.**
   Implement `doUnsubscribe()`, `doDisconnect()`, `doReconnect()`, `doResubscribe()`.

   **For other recovery patterns — implement `RecoveryStrategy` directly.**
   Signal exactly once via `RecoveryContext`.

9. **Assign `VenueEnum.XLN`** — add the constant with the next available byte
   value, correct `BookDepth`, and correct `TemplateId`. Immutable once assigned
   — see Section 23.

10. **Register in ServiceLoader** — add one line:
    `io.rueishi.marketdata.crypto.venue.<exchange>.l<n>.XLnConnectorFactory`

11. **Add test fixtures** under `src/test/resources/venue/<exchange>/l<n>/`.
    For `REST_THEN_DELTA` also add `snapshot_rest.json` and delta fixtures.

12. **Add `README.md`** — document channel, snapshot mode, sequence mapping,
    auth scheme, `VenueEnum` constant, `BookDepth`, `TemplateId`.

### Example: Adding `BINANCE_L2`

```
venue/binance/
  l2/
    BinanceL2Connector.java           # ~10 lines — extends AbstractConnector
    BinanceL2ConnectorFactory.java    # ~20 lines — extends AbstractConnectorFactory
    BinanceL2FeedParser.java          # ~80 lines — genuine parse logic (U/u fields)
    BinanceL2SnapshotStrategy.java    # ~60 lines — REST_THEN_DELTA alignment
    BinanceL2RecoveryStrategy.java    # ~20 lines — extends ReconnectRecoveryStrategy
    BinanceL2SubscriptionBuilder.java # ~30 lines — HMAC auth + JSON construction
    README.md
  shared/
    BinanceAuthenticator.java
    BinanceConfig.java
```

```java
// BinanceL2Connector.java — complete class
public final class BinanceL2Connector extends AbstractConnector {

    public BinanceL2Connector(InstrumentConfig instrument,
                              BinanceL2FeedParser parser,
                              BinanceL2SnapshotStrategy snapshot,
                              BinanceL2RecoveryStrategy recovery,
                              BinanceL2SubscriptionBuilder subscription) {
        super(instrument, parser, snapshot, recovery, subscription);
    }

    @Override protected VenueEnum venueEnum()     { return VenueEnum.BINANCE_L2; }
    @Override protected int       maxEntryCount() { return 10_000; }
    @Override protected int       headroomBytes() { return 8_192;  }
    @Override protected String    venueEndpoint() {
        return "wss://stream.binance.com:9443/ws";
    }
}
```

```java
// BinanceL2ConnectorFactory.java — complete class
public final class BinanceL2ConnectorFactory extends AbstractConnectorFactory {

    @Override public VenueEnum venue() { return VenueEnum.BINANCE_L2; }

    @Override
    protected BinanceConfig parseVenueConfig(Map<String, Object> raw) {
        return BinanceConfig.parse(raw);
    }

    @Override
    protected Connector createConnector(InstrumentConfig instrument,
                                        GatewayConfig config,
                                        Object venueConfig) {
        BinanceConfig cfg  = (BinanceConfig) venueConfig;
        BinanceAuthenticator auth = new BinanceAuthenticator(cfg);
        return new BinanceL2Connector(
            instrument,
            new BinanceL2FeedParser(),
            new BinanceL2SnapshotStrategy(cfg.restEndpoint()),
            new BinanceL2RecoveryStrategy(auth),
            new BinanceL2SubscriptionBuilder(auth)
        );
    }
}
```

```java
// BinanceL2RecoveryStrategy.java — complete class
public final class BinanceL2RecoveryStrategy extends ReconnectRecoveryStrategy {

    private final BinanceAuthenticator auth;

    public BinanceL2RecoveryStrategy(BinanceAuthenticator auth) {
        this.auth = auth;
    }

    @Override protected void doUnsubscribe() { /* send unsubscribe frame */ }
    @Override protected void doDisconnect()  { /* close WebSocket channel */ }
    @Override protected void doReconnect()   { /* dial with backoff+jitter */ }
    @Override protected void doResubscribe() { /* send subscribe frame with HMAC */ }
}
```

```
# META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory
io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2ConnectorFactory
io.rueishi.marketdata.crypto.venue.coinbase.l3.CoinbaseL3ConnectorFactory
io.rueishi.marketdata.crypto.venue.binance.l2.BinanceL2ConnectorFactory   ← add this line only
```

No changes to `AbstractConnector`, `AbstractConnectorFactory`, `SbeEncoder`,
`Publisher`, `GatewayBootstrap`, `VenueRegistry`, or any other `core/` class
are required beyond adding the `VenueEnum` constant.

---

## 23. Venue Enum Allocation

The `venue` field in the SBE binary is a `uint8`. Each value encodes
**exchange + data level** as a single atomic identity — there is no separate
`bookDepth` configuration dimension. Values are assigned once and are
**immutable** — changing an assigned value is a breaking wire format change.

The `bookDepth` field in the SBE body carries the same depth level information
in a self-describing form so downstream consumers can decode messages without
a lookup table of venue byte values.

```java
public enum VenueEnum {
    COINBASE_L2  ((byte)  1, BookDepth.L2, TemplateId.BOOK_LEVEL),
    COINBASE_L3  ((byte)  2, BookDepth.L3, TemplateId.ORDER_ENTRY),
    BINANCE_L2   ((byte)  3, BookDepth.L2, TemplateId.BOOK_LEVEL),
    BINANCE_L3   ((byte)  4, BookDepth.L3, TemplateId.ORDER_ENTRY),
    KRAKEN_L2    ((byte)  5, BookDepth.L2, TemplateId.BOOK_LEVEL),
    OKX_L2       ((byte)  6, BookDepth.L2, TemplateId.BOOK_LEVEL),
    OKX_L3       ((byte)  7, BookDepth.L3, TemplateId.ORDER_ENTRY),
    BYBIT_L2     ((byte)  8, BookDepth.L2, TemplateId.BOOK_LEVEL);
    // values 9–254 available for future venue+level combinations
    // value 0   reserved — invalid/unset sentinel
    // value 255 reserved — future use

    private final byte       byteValue;
    private final BookDepth  bookDepth;
    private final TemplateId templateId;

    VenueEnum(byte byteValue, BookDepth bookDepth, TemplateId templateId) {
        this.byteValue  = byteValue;
        this.bookDepth  = bookDepth;
        this.templateId = templateId;
    }

    public byte       byteValue()  { return byteValue;  }
    public BookDepth  bookDepth()  { return bookDepth;  }
    public TemplateId templateId() { return templateId; }
}
```

**Assigned values**

| Value | Constant | BookDepth | TemplateId | Status |
|---|---|---|---|---|
| `0` | Reserved — invalid/unset sentinel | — | — | Reserved |
| `1` | `COINBASE_L2` | L2 | BOOK_LEVEL | **Assigned** |
| `2` | `COINBASE_L3` | L3 | ORDER_ENTRY | **Assigned** |
| `3` | `BINANCE_L2` | L2 | BOOK_LEVEL | Pre-allocated |
| `4` | `BINANCE_L3` | L3 | ORDER_ENTRY | Pre-allocated |
| `5` | `KRAKEN_L2` | L2 | BOOK_LEVEL | Pre-allocated |
| `6` | `OKX_L2` | L2 | BOOK_LEVEL | Pre-allocated |
| `7` | `OKX_L3` | L3 | ORDER_ENTRY | Pre-allocated |
| `8` | `BYBIT_L2` | L2 | BOOK_LEVEL | Pre-allocated |
| `9–254` | Available for future venue+level strategies | — | — | Available |
| `255` | Reserved — future use | — | — | Reserved |

Pre-allocated values have no implementation yet. They are reserved to prevent
collision when two teams implement different venue strategies independently.

Each `ConnectorFactory` passes `venue().byteValue()`, `venue().bookDepth().byteValue()`,
and `venue().templateId().byteValue()` to `SbeEncoder` at construction. The
connector and parser never reference these values directly.

---

## 24. Test Strategy

### Test Categories

1. **Parser Tests** — fixture-driven; validate field extraction from `ByteBuf`;
   validate `product_id` match/mismatch behavior; validate pre-snapshot drops

2. **Encoding Tests** — byte-level verification; little-endian layout; schema
   correctness; `blockLength`-driven decoding; `gatewayMessageSeq` monotonicity
   within a connector session

3. **Sequence Mapping Tests** — verify per-venue `seq1`/`seq2` assignment;
   gateway-managed vs exchange-native; reset on recovery

4. **Buffer Reuse Tests** — verify no per-message encode allocation; exercise
   1,000,000-message reuse; verify oversize messages rejected without allocation

5. **Context Lifecycle Tests** — verify `resetSession()` renews `SequenceTracker`
   and `SnapshotGatekeeper`; verify shared gatekeeper invariant between
   `parseCtx` and `snapshotCtx`; verify `ConnectorContext` not held after `init()`

6. **Autonomous Recovery Tests** — stream-integrity detection and self-triggered
   recovery; heartbeat timeout; pre-snapshot update drops; session boundary behavior

7. **Downstream Recovery Tests** — downstream-triggered reset/resnapshot; resync
   flow; duplicate request coalescing; venue mismatch ignored

8. **Publisher and Backpressure Tests** — bounded retry behavior; drop on
   backpressure; backpressure triggers recovery; event-loop thread does not block

9. **Protocol Violation Tests** — `product_id` mismatch; unknown symbol; malformed
   JSON; missing required fields; invalid numeric formats

10. **Replay / End-to-End** — recorded feed sequences; validate encoded binary
    output; validate recovery interactions; liveness-triggered reconnect behavior

### Fixtures

Stored under `src/test/resources/venue/<venue>/`. Each venue must provide:
`snapshot`, update message, `heartbeat`, `subscriptions_ack`, `malformed`, `unknown_type`.

### Coinbase L2 End-to-End Simulator

Venue-level Phase 2 and Phase 3 end-to-end verification must include a reusable
test-only Coinbase L2 channel simulator so the Coinbase venue path can be
exercised end to end without contacting the live Coinbase Exchange Direct Feed.

Core transport tests are different: they should continue to use a generic local
Netty WebSocket server because they verify `core/transport` behavior without
venue semantics. The Coinbase simulator must not replace generic core transport
test servers.

The simulator is **test infrastructure only**. It must live under
`src/test/java/.../venue/coinbase/l2/` and must not be imported by production
code. The recommended class name is:

```java
// src/test/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseExchangeL2Server.java
final class CoinbaseExchangeL2Server implements AutoCloseable {
    URI uri();

    String awaitSubscribe();
    String awaitUnsubscribe();

    void sendValidSubscriptionsAck();
    void sendBadSubscriptionsAckMissingLevel2();
    void sendBadSubscriptionsAckMissingHeartbeat();
    void sendBadSubscriptionsAckWrongProduct();
    void sendSubscriptionsAckWithExtraChannel();

    void sendSnapshot();
    void sendSnapshotWithoutTime();
    void sendSnapshotEpochZeroTime();
    void sendSnapshotWrongProduct();

    void sendL2UpdateBuyUpsert();
    void sendL2UpdateSellDelete();
    void sendL2UpdateMultiChange();
    void sendL2UpdateBeforeSnapshot();
    void sendL2UpdateWrongProduct();

    void sendHeartbeat();
    void sendHeartbeatWrongProduct();

    void sendUnknownType();
    void sendMalformed();
    void closeClientConnection();
}
```

The simulator must use an in-process Netty WebSocket server bound to
`127.0.0.1` on an ephemeral port. It must capture inbound client text frames
and expose deterministic `awaitSubscribe()` / `awaitUnsubscribe()` helpers that
block with a bounded timeout. It must send outbound frames as
`TextWebSocketFrame` payloads loaded from fixture files or embedded constants.
Tests must still use the production `NettyWebSocketTransport`, production
`CoinbaseL2ConnectorFactory`, production `CoinbaseL2Connector`,
`InMemoryPublisher`, and `SbeDecoder`.

The simulator replaces duplicated private local WebSocket server test helpers
inside Coinbase L2 venue integration tests only. It is intentionally
Coinbase-L2-domain-aware: venue tests should express feed scripts in terms of
Coinbase events rather than raw socket operations.

#### Required Simulator Test Data

The Coinbase L2 simulator must cover the following deterministic messages.
Fixture files may live under the existing repository fixture layout
`src/test/resources/fixtures/coinbase/l2/` or under the venue-oriented layout
`src/test/resources/venue/coinbase/l2/`, but the test suite must use one
consistent path and document it in the simulator.

Control-plane fixtures:
- `subscriptions_ack.json`:
  - `type = "subscriptions"`
  - contains a `level2` channel for `BTC-USD`
  - contains a `heartbeat` channel for `BTC-USD`
  - may include extra Coinbase fields not used by the parser
- `subscriptions_ack_extra_channel.json`:
  - same as valid ack, plus an unrelated extra channel
  - expected result: accepted, required channels still validated
- `subscriptions_ack_missing_level2.json`:
  - missing required `level2`
  - expected result: `subscriptionValidationFailures` increments and reset
    recovery is requested
- `subscriptions_ack_missing_heartbeat.json`:
  - missing required `heartbeat`
  - expected result: `subscriptionValidationFailures` increments and reset
    recovery is requested
- `subscriptions_ack_wrong_product.json`:
  - has required channels but not the configured `product_id`
  - expected result: validation failure and reset recovery request

Market-data happy-path fixtures:
- `snapshot.json`:
  - `type = "snapshot"`
  - `product_id = "BTC-USD"`
  - top-level non-zero RFC 3339 `time`
  - at least one bid row and one ask row
  - expected encoded output: `BOOK_SNAPSHOT`, `venue = COINBASE_L2`,
    `bookDepth = 2`, `templateId = BOOK_LEVEL`, `entryCount = bid count + ask count`,
    every row `action = UPSERT`
- `l2update_buy_upsert.json`:
  - `type = "l2update"`
  - one `changes` row with side `"buy"` and size `> 0`
  - expected encoded output: `BOOK_UPDATE`, `side = BID`, `action = UPSERT`
- `l2update_sell_delete.json`:
  - one `changes` row with side `"sell"` and size `"0"` or equivalent zero
  - expected encoded output: `BOOK_UPDATE`, `side = ASK`, `action = DELETE`,
    quantity mantissa `0`
- `l2update_multi_change.json`:
  - multiple `changes` rows mixing buy/sell and upsert/delete
  - expected encoded output: `entryCount` equals exact number of change rows
- `l2update_duplicate_price.json`:
  - two `changes` entries with the **same price and same side** in one message,
    e.g. `["buy","50000.00","1.5"]` followed by `["buy","50000.00","0"]`
  - expected encoded output: `entryCount = 2`; first entry `side=BID action=UPSERT`;
    second entry `side=BID action=DELETE`; both entries encoded in arrival order
  - must NOT deduplicate — a parser that overwrites or skips the first entry fails
    this test; the SBE repeating group must contain exactly two entries
- `heartbeat.json`:
  - `type = "heartbeat"`
  - matching `product_id`
  - expected result: heartbeat counters and last-heartbeat timestamp update;
    no SBE market-data message is published

Drop/rejection fixtures:
- `snapshot_without_time.json`:
  - valid snapshot without top-level `time`
  - expected encoded output: `exchangeTimestamp = -1`
- `snapshot_epoch_zero_time.json`:
  - valid snapshot with epoch-zero timestamp
  - expected encoded output: `exchangeTimestamp = -1`
- `snapshot_wrong_product.json`:
  - valid shape but wrong `product_id`
  - expected result: product mismatch / unknown symbol counters increment;
    no publish
- `l2update_before_snapshot.json`:
  - valid update before the snapshot gate is open
  - expected result: `preSnapshotDrops` increments; no buffering and no publish
  - because Coinbase guarantees normal order after a clean subscribe, this is a
    technical anomaly path; do not require recovery unless a separate recovery
    policy explicitly owns that behavior
- `l2update_wrong_product.json`:
  - valid update shape but wrong `product_id`
  - expected result: counted and dropped; no publish
- `heartbeat_wrong_product.json`:
  - heartbeat for a different product
  - expected result: counted and dropped; no publish
- `unknown_type.json`:
  - well-formed JSON with unsupported `type`
  - expected result: unknown-type counter increments; no publish
- `error_message.json`:
  - `{"type":"error","message":"Failed to subscribe","reason":"authentication failure"}`
  - expected result: `authenticationErrors` counter increments; recovery is
    triggered with reason `STREAM_INTEGRITY_FAILURE`; no book publish
- `malformed.json`:
  - malformed JSON or malformed required arrays
  - expected result: parse-failure / malformed counters increment; no publish
- `bad_decimal.json`:
  - valid message envelope but an invalid decimal value in price or size
  - expected result: malformed counter increments; no publish

Recovery-script fixtures:
- `recovery_snapshot.json`:
  - same contract as `snapshot.json`, used after reset/reconnect/resubscribe
- `recovery_l2update.json`:
  - same contract as `l2update_*`, used both before and after the recovery
    snapshot to verify pre-snapshot drops and post-snapshot publishing

#### Required End-to-End Scenarios

The simulator-backed integration suite must cover these scenarios with the real
production Coinbase L2 connector path:

1. **Connect / subscribe / ack / publish**
   - bootstrap or manually construct a `CoinbaseL2Connector` through
     `CoinbaseL2ConnectorFactory`
   - connect to `simulator.uri()`
   - `awaitSubscribe()` and assert:
     - `"type":"subscribe"`
     - `"product_ids":["BTC-USD"]`
     - `"channels":["level2","heartbeat"]` or equivalent channel membership
     - auth fields are present (`signature`, `key`, `passphrase`, timestamp)
   - send valid `subscriptions_ack`
   - send `snapshot`
   - send one or more `l2update` fixtures
   - decode with `SbeDecoder` and assert wire-format fields and entry values

2. **Heartbeat and liveness**
   - after valid ack, assert liveness monitoring starts
   - send `heartbeat`
   - assert heartbeat counters update and no message is published
   - advance/inject time or call the exposed liveness test hook to verify stalled
     heartbeat detection triggers the Phase 3 recovery path

3. **Protocol violations**
   - send unknown type, malformed frame, bad decimal, and product mismatches
   - assert the relevant counters increment
   - assert no SBE message is published and the pipeline remains usable

4. **Recovery ordering**
   - establish a normal snapshot/update session
   - request downstream or autonomous recovery
   - assert publisher captures `BOOK_RESET`
   - assert simulator receives unsubscribe followed by a fresh subscribe
   - send a recovery update before the recovery snapshot; assert it is dropped
   - send `recovery_snapshot`; assert recovery completion and snapshot publish
   - send `recovery_l2update`; assert update publish
   - assert the published event order is exactly
     `BOOK_RESET -> BOOK_SNAPSHOT -> BOOK_UPDATE` for the recovery segment

5. **Shutdown**
   - call connector or runtime shutdown
   - assert simulator receives unsubscribe when the connection is still open
   - assert best-effort `BOOK_RESET` is published
   - assert the active WebSocket connection gauge decrements to zero
   - assert new recovery requests are rejected after runtime shutdown begins

6. **Bootstrap/runtime wiring**
   - add at least one test that initializes through `GatewayBootstrap` /
     `GatewayRuntime` with `venue = COINBASE_L2`, simulator endpoint in the
     Coinbase venue config, `IN_MEMORY` publisher, and test observability paths
   - start the returned connector(s), drive simulator frames, and verify
     publisher/counter output
   - this proves config -> ServiceLoader/registry -> factory -> connector ->
     transport -> parser -> encoder -> publisher -> counters wiring, not just
     parser-level or transport-level behavior

The simulator suite must remain deterministic. It must not contact external
Coinbase services, must not require API credentials beyond test constants, and
must use bounded waits for every asynchronous expectation.

### `SbeDecoder` — Test Utility

`SbeDecoder` is a test-only class in `src/test/java/.../core/encoding/SbeDecoder.java`.
It is not a production class and must never be imported by any non-test source.

It reads the binary layout defined in Section 17 and exposes decoded fields as
Java primitives for test assertions:

```java
// src/test/java/io/rueishi/marketdata/crypto/core/encoding/SbeDecoder.java
public final class SbeDecoder {

    public SbeDecoder(byte[] encoded) { ... }     // wraps a captured InMemoryPublisher message

    // Header fields
    public int  magic()        { ... }  // uint16
    public int  version()      { ... }  // uint8
    public int  templateId()   { ... }  // uint8 — 1=BOOK_LEVEL, 2=ORDER_ENTRY
    public int  blockLength()  { ... }  // uint16
    public int  entryCount()   { ... }  // uint16

    // Body fields
    public int  eventType()    { ... }  // uint8 — 1=RESET, 2=SNAPSHOT, 3=UPDATE
    public int  venue()        { ... }  // uint8
    public int  bookDepth()    { ... }  // uint8
    public long instrumentId() { ... }  // uint32
    public long gatewayMessageSeq() { ... } // uint64
    public long seq1()         { ... }  // uint64
    public long seq2()         { ... }  // uint64
    public long exchangeTimestamp()  { ... } // int64, -1 if absent
    public long ingressTimestamp()   { ... } // int64

    // BOOK_LEVEL repeating group access (templateId=1)
    public int  side(int index)          { ... } // uint8
    public int  action(int index)        { ... } // uint8
    public int  priceScale(int index)    { ... } // uint8
    public int  qtyScale(int index)      { ... } // uint8
    public long priceMantissa(int index) { ... } // int64
    public long qtyMantissa(int index)   { ... } // int64

    // ORDER_ENTRY repeating group access (templateId=2)
    public long orderId(int index)       { ... } // uint64
    public int  orderSide(int index)     { ... } // uint8
    public int  orderAction(int index)   { ... } // uint8
    public int  orderType(int index)     { ... } // uint8
    public int  orderPriceScale(int index)   { ... }
    public int  orderQtyScale(int index)     { ... }
    public long orderPriceMantissa(int index){ ... }
    public long orderQtyMantissa(int index)  { ... }
}
```

Usage pattern in encoding tests:
```java
InMemoryPublisher pub = new InMemoryPublisher();
// ... drive parser with fixture ByteBuf ...
SbeDecoder decoded = new SbeDecoder(pub.lastMessage());
assertThat(decoded.eventType()).isEqualTo(2);          // BOOK_SNAPSHOT
assertThat(decoded.bookDepth()).isEqualTo(2);          // L2
assertThat(decoded.entryCount()).isEqualTo(2);
assertThat(decoded.priceMantissa(0)).isEqualTo(2192173L); // "21921.73"
assertThat(decoded.priceScale(0)).isEqualTo(2);
```

`ByteBufScanner.parseDecimal()` must be unit tested independently with at minimum:
- Standard price: `"21921.73"` → mantissa=2192173, scale=2
- Leading zero fraction: `"0.06317902"` → mantissa=6317902, scale=8
- Trailing zero fraction: `"21922.10"` → mantissa=2192210, scale=2
- Integer only: `"100"` → mantissa=100, scale=0
- Zero: `"0"` → mantissa=0, scale=0
- Negative: `"-0.5"` → mantissa=-5, scale=1
- Large value near long boundary: verify no floating-point rounding
- Overflow sentinel: value exceeding Long.MAX_VALUE → mantissa=Long.MAX_VALUE, scale=0

### Assertions

- Parser: correct field extraction from direct buffer; product_id handling
- Encoder: exact byte layout; `blockLength` semantics; `gatewayMessageSeq` monotonicity
- Sequence: correct venue mapping; reset to 1 after recovery
- Buffer: deterministic reuse behavior; encode buffer reuse count == message count
- Context: shared gatekeeper opens parser gate; `resetSession()` clears state
- Recovery: correct signal generation; BOOK_RESET → BOOK_SNAPSHOT → BOOK_UPDATE ordering;
  duplicate coalescing; heartbeat timeout and backpressure recovery paths

---

## 25. Acceptance Criteria

The gateway is complete when all of the following are demonstrably met through
automated tests. Each criterion is binary pass/fail.

### Functional Correctness

- **AC-1**: The gateway connects to a Coinbase WebSocket endpoint (live or mock),
  authenticates, subscribes to `level2` and `heartbeat` for a configured instrument,
  and receives inbound messages.

- **AC-2**: An inbound Coinbase L2 `snapshot` is parsed, encoded, and published with:
  `eventType = BOOK_SNAPSHOT`, correct `instrumentId`, `venue = COINBASE_L2` (byte 1),
  `bookDepth = L2` (byte 2), `templateId = BOOK_LEVEL` (byte 1),
  `gatewayMessageSeq = seq1 = seq2 = 1` for the first message of the session,
  correct `exchangeTimestamp`, populated `ingressTimestamp`,
  `entryCount` in the encoded header equals the number of bid+ask entries in the
  snapshot payload (verified by `SbeDecoder.entryCount()` on the captured message),
  every level with correct `side`, `action = UPSERT`, and correct mantissa/scale fields.

- **AC-3**: An inbound Coinbase `l2update` is parsed, encoded, and published with
  `eventType = BOOK_UPDATE`. Rows with size `> 0` encode `action = UPSERT`. Rows
  with size `0` encode `action = DELETE`.

- **AC-4**: An inbound Coinbase `heartbeat` updates health state and observability
  counters but does not produce an encoded market-data message.

- **AC-5**: Inbound messages with unknown or unsupported type/channel combinations
  are silently ignored, counted, and do not cause exceptions or pipeline failure.

- **AC-6**: Inbound messages with a `product_id` that does not match the connector's
  subscribed instrument are rejected, counted, and not published.

- **AC-7**: Inbound symbols that cannot be resolved to a configured `instrumentId`
  are dropped before encoding, counted, and not published.

- **AC-8**: `gatewayMessageSeq` is monotonically increasing starting at 1 for each
  connector session and increments by 1 for every successfully published message.

- **AC-9**: Each inbound `subscriptions` acknowledgement is validated per the rules
  in Section 14. Both `level2` and `heartbeat` channels must be present with the
  correct `product_id`. A missing channel or wrong product ID is a control-plane
  error: increments the subscription validation failure counter and emits a
  structured log entry. Extra channels in the ack are tolerated.

- **AC-9a**: An inbound `subscriptions` acknowledgement with a missing required
  channel or wrong `product_id` is treated as a control-plane failure that
  triggers recovery.

- **AC-10**: The gateway sends authenticated `subscribe` for `level2` and `heartbeat`
  after each connection establishment.

- **AC-11**: The gateway sends authenticated `unsubscribe` before closing connections
  during recovery and graceful shutdown.

### Sequence Tracking and Stream Integrity

- **AC-12**: The gateway tracks snapshot boundaries and connector-local normalized
  sequencing per connection so stream state remains internally consistent.

- **AC-13**: When a stream-integrity failure is detected, the gateway autonomously
  initiates recovery without waiting for a downstream request.

- **AC-14**: When an invalid or out-of-order snapshot/update transition is detected,
  the gateway treats it as a stream integrity failure and initiates recovery.

- **AC-15**: After subscribe or resubscribe, inbound updates are dropped (not buffered)
  until the first snapshot is received and accepted.

- **AC-16**: The first accepted snapshot after connect or recovery establishes the
  clean book boundary. The gateway does not assume an exchange-provided starting
  sequence is present in Coinbase level2 payloads.

### Sequence Mapping

- **AC-17**: For Coinbase, `gatewayMessageSeq`, `seq1`, and `seq2` are all equal to
  `sequenceTracker().next()` for every published message. Verified by byte-level
  inspection of encoded output.

- **AC-18**: `sequenceTracker().current()` returns 1 for the first message after
  construction or recovery. Returns N for the Nth published message of the session.

### Recovery

- **AC-19**: When recovery is initiated (stream-integrity failure, heartbeat timeout,
  backpressure, or downstream request), BOOK_RESET is published before the venue
  recovery sequence begins.

- **AC-20**: The Coinbase recovery sequence is: best-effort unsubscribe → close →
  reconnect → authenticate → resubscribe → await fresh snapshot.

- **AC-21**: After recovery: BOOK_RESET → BOOK_SNAPSHOT → BOOK_UPDATE. No interleaving.

- **AC-22**: While recovery is in progress for an instrument, duplicate recovery
  requests for the same instrument are logged and ignored — no duplicate BOOK_RESET,
  reconnects, or resubscribes.

- **AC-23**: Downstream recovery requests where `RecoveryRequest.venue` does not
  match the configured instance venue are silently ignored and not acted upon.
  Verified by sending a well-formed `RecoveryRequest` with a non-matching venue
  and confirming no BOOK_RESET is published, no recovery is initiated, and the
  connector's recovery-in-progress flag is not set.

- **AC-24**: Recovery initiated by heartbeat timeout follows the same path as
  other self-triggered stream-integrity recoveries.

- **AC-25**: Recovery initiated by publisher backpressure follows the same path.

### Context Lifecycle

- **AC-26**: No venue implementation (`FeedParser`, `SnapshotStrategy`,
  `RecoveryStrategy`) holds any context interface as a field. Verified by ArchUnit.

- **AC-27**: `DefaultParseContext` is allocated exactly once per connector — in
  `init()`. It is never replaced across recoveries. Verified by checking that the
  object reference held by the connector after N recoveries is the same instance
  created during `init()`.

- **AC-28**: After every recovery, `parseCtx.resetSession()` is called before
  `sendSubscribe()`. Verified by confirming `sequenceTracker().current()` returns
  1 on the first published message of the new session, and that `snapshotGatekeeper()`
  returns a new instance (not the one from the previous session).

- **AC-29**: After every recovery Phase A (`onChannelRestored()` fires), `snapshotCtx`
  is a new `DefaultSnapshotContext` instance whose `snapshotGatekeeper()` is the same
  object returned by `parseCtx.currentSnapshotGatekeeper()` after `resetSession()`.
  Verified by confirming `snapshotCtx.snapshotGatekeeper().accept()` causes
  `parseCtx.snapshotGatekeeper().isReady()` to return `true` immediately.

- **AC-30**: After every recovery Phase A, `recoveryCtx` is a new
  `DefaultRecoveryContext` instance. A `RecoveryContext` from a previous attempt
  cannot trigger `onChannelRestored()` for a subsequent attempt.

- **AC-31**: `RecoveryStrategy.execute()` calls exactly one of `onChannelRestored()`
  or `onRecoveryFailed()` per invocation — never both, never neither. Verified by
  counting callback invocations across success, failure, and exception scenarios.

- **AC-31a**: `recoveryInProgress` is `true` after `recover()` is called and remains
  `true` after `onChannelRestored()` fires. It becomes `false` only after
  `SnapshotContext.onSnapshotBoundaryAccepted()` fires. Verified by a test that
  issues a second recovery request between Phase A and Phase B completion and
  confirms it is coalesced (not acted upon).

- **AC-31b**: `recoveryCompletions` counter is incremented exactly once per
  completed recovery, and only after `onSnapshotBoundaryAccepted()` fires — not
  when `onChannelRestored()` fires. Verified by confirming the counter value does
  not change between Phase A and Phase B in a controlled test.

- **AC-32**: `ConnectorContext` is not held as a field by any `Connector`
  implementation after `init()` returns.

### Performance and Allocation

- **AC-33**: The hot-path pipeline (parse → encode → publish) produces no per-message
  heap allocation under sustained load. Verified by thread-level allocation measurement
  across 1,000,000+ messages.

- **AC-33a**: A synchronous publisher implementation does not store the `DirectBuffer`
  reference passed to `publish()` in any field. Verified by code review and by
  confirming that the buffer contents read after a subsequent `beginMessage()` call
  reflect the new message, not the previous one.

- **AC-33b**: An asynchronous publisher implementation copies the bytes
  `[offset, offset+length)` into its own pre-allocated off-heap slot before
  `publish()` returns. Verified by inspecting the copy destination after
  `publish()` returns and before the next `beginMessage()` call and confirming
  the copy matches the original encoded bytes.

- **AC-34**: The off-heap encode buffer is reused across all messages within a connector
  session. `SbeEncoder.reuseCount()` equals the total published message count.

- **AC-35**: No per-message encode buffer allocation. Buffer allocated once in `init()`.

- **AC-36**: Messages exceeding 10,000 entries are rejected without allocating a larger
  buffer.

- **AC-36b**: The `entryCount` field in the encoded header equals the exact number of
  `writeLevel()` or `writeOrder()` calls made between `beginMessage()` and
  `endMessage()`. Verified by encoding a known fixture with N entries and confirming
  `SbeDecoder.entryCount() == N`. The encoder must produce a correct count even
  when N == 0 (zero-entry snapshot/update fixture) and when N == 1 (single-entry update).

- **AC-36c**: A publisher-encoded `BOOK_RESET` writes `entryCount = 0` directly in the
  header without using `SbeEncoder.endMessage()`. Verified by capturing a reset message
  and confirming `eventType = BOOK_RESET` and `SbeDecoder.entryCount() == 0`.

- **AC-36d**: A publisher-encoded L3 `BOOK_RESET` preserves venue template semantics.
  Verified by triggering `publishReset()` for an L3 venue (for example `COINBASE_L3`)
  and confirming the captured message has `eventType = BOOK_RESET`, the correct
  `venue` byte, `bookDepth = 3`, `templateId = ORDER_ENTRY (2)`, and
  `SbeDecoder.entryCount() == 0`.

- **AC-36a**: `ByteBufScanner.parseDecimal()` produces the correct (mantissa, scale) pair
  for all test vectors listed in Section 24 without calling `Double.parseDouble` or any
  floating-point operation. Verified by unit tests covering standard, leading-zero,
  trailing-zero, integer, negative, large-value, and overflow cases. Large-value test
  uses a price string that would round if parsed as a `double` and confirms the integer
  mantissa is exact.

- **AC-37**: The `CoinbaseL2FeedParser` does not allocate `String`, `JsonNode`, `Map`,
  `List`, `Instant`, `DateTimeFormatter`, or intermediate JSON objects on the hot path.

- **AC-38**: Timestamp parsing converts RFC 3339 timestamps to epoch nanoseconds
  without allocating `java.time` objects.

- **AC-39**: Symbol resolution from `ByteBuf` does not allocate `String` on the hot path.

- **AC-40**: Ingress-to-handoff latency is tracked via min, max, and last counters
  updated immediately before every successful `publish()` call.

- **AC-40a**: Every `Publisher` implementation updates publisher-stage latency via
  min, max, and last counters measured from entry to return of `publish()`
  using the passed `NanoClock`. For asynchronous publishers this measures
  enqueue/copy time only, not downstream consumer completion latency. In the
  current revision, values may be zero or quantized within one event-loop
  iteration because `CachedNanoClock` is refreshed once per iteration.
  `publishReset()` during recovery/shutdown must not update these counters.

### Observability

- **AC-41**: All required counter categories are allocated with descriptive labels via
  Agrona `CountersManager` backed by a memory-mapped buffer.

- **AC-42**: An HTTP `/metrics` endpoint serves Prometheus-compatible text via generic
  `CountersReader` iteration, running on a non-hot-path thread.

- **AC-43**: New counters allocated through the shared `CountersManager` are
  automatically discoverable by the metrics endpoint without code changes.

- **AC-44**: Per-instrument counters include `instrumentId` in their labels.

- **AC-45**: The active WebSocket connections gauge increments on establishment and
  decrements on close.

- **AC-46**: If the JVM terminates unexpectedly, the last counter values remain
  readable from the memory-mapped buffer.

- **AC-47**: Hot-path error recording uses Agrona `DistinctErrorLog`.

### Transport and Lifecycle

- **AC-48**: On Linux with Netty epoll available, the gateway uses Netty native
  epoll transport and BoringSSL via `netty-tcnative`. When native epoll or
  OpenSSL is unavailable, the gateway falls back to the portable Netty/JDK
  provider path and that fallback is covered by tests.

- **AC-49**: On graceful shutdown (`SIGTERM`), the gateway sends unsubscribe, publishes
  best-effort BOOK_RESET per instrument, closes connections, and exits within the
  configured deadline.

- **AC-50**: Heartbeat liveness monitoring detects stalled connections and triggers
  recovery after the configured timeout.

- **AC-51**: Reconnect uses exponential backoff with jitter, resetting after a
  successful connection.

- **AC-52**: All time-dependent behavior (heartbeat timeout, recovery duration) is
  testable via injectable clock abstractions without waiting for real wall-clock time.

### Wire Format

- **AC-53**: Encoded messages are little-endian with the exact byte layout from Section 16.
  Body is 47 bytes (includes the `bookDepth` field).

- **AC-54**: The decoder uses `blockLength` from the header to locate the repeating
  group — not hard-coded offsets.

- **AC-55**: Epoch-zero exchange timestamps are encoded as `-1`, not `0`.

- **AC-56**: The `exchangeTimestamp` for Coinbase L2 snapshot messages uses the
  top-level `time` field. If the field is absent or epoch-zero, `-1` is encoded.
  Per-level timestamp fields are never used for snapshot `exchangeTimestamp`.

- **AC-57**: Every encoded message carries the correct `bookDepth` byte in the body.
  A `COINBASE_L2` connector encodes `bookDepth = 2`. A `COINBASE_L3` connector
  encodes `bookDepth = 3`. Verified by byte-level inspection of encoded output.

- **AC-58**: Every encoded message carries the correct `templateId` byte in the header.
  A `COINBASE_L2` connector encodes `templateId = 1` (BOOK_LEVEL). A `COINBASE_L3`
  connector encodes `templateId = 2` (ORDER_ENTRY). Verified by byte-level inspection.

- **AC-59**: `VenueEnum` constants carry consistent `bookDepth` and `templateId`
  values — verified by confirming `venue.bookDepth().byteValue()` matches the
  `bookDepth` field in every message produced by a connector for that venue, and
  `venue.templateId().byteValue()` matches the `templateId` field in the header.

- **AC-60**: A `BOOK_LEVEL` encoded message uses `writeLevel()` entries of 20 bytes
  each. An `ORDER_ENTRY` encoded message uses `writeOrder()` entries of 29 bytes
  each. The total encoded size matches the formula from Section 16 for the configured
  template.

### Project Structure

- **AC-61**: The Maven project has no imports from `core/` to any `venue/` package,
  except through `ServiceLoader`. Verified by ArchUnit.

- **AC-62**: Each `venue/<exchange>/l<n>/` sub-package contains complete implementations
  of `Connector`, `ConnectorFactory`, `FeedParser`, `SubscriptionBuilder`,
  `SnapshotStrategy`, and `RecoveryStrategy` for its specific exchange + depth level.

- **AC-63**: `VenueRegistry` references no venue implementation class by name.
  All discovery is via `ServiceLoader`.

### Configuration

- **AC-64**: The gateway refuses to start if `venue` is missing or unrecognized.

- **AC-65**: The gateway refuses to start if `instruments` is empty.

- **AC-66**: The gateway refuses to start if any two instruments share the same
  `exchangeSymbol` or `instrumentId`.

- **AC-67**: Auth credentials that resolve to empty string cause a startup failure
  with a structured error that does not log the credential value.

- **AC-68**: The number of Connectors created at startup equals the number of
  entries in `instruments`.

---

## 26. Implementation Roadmap

The system is built in four phases. Each phase delivers a runnable, testable
milestone. Do not proceed to the next phase until all acceptance criteria
listed for that phase pass.

### Phase 1 — Core Infrastructure (no network, no venue code)

**Goal:** All `core/` classes exist, compile, and pass unit tests using
in-memory stubs. No venue packages. No WebSocket. No Netty.

Deliverables:
- `SbeEncoder` with `writeLevel()` and `writeOrder()` — byte-layout tests pass for both templates
- `SequenceTracker`, `SnapshotGatekeeper` — unit tested including reset behaviour
- `DefaultParseContext`, `DefaultSnapshotContext`, `DefaultRecoveryContext` — lifecycle tests pass
- `AbstractConnector`, `AbstractConnectorFactory` — compilable with a stub venue that uses them
- `SubscribeDrivenSnapshotStrategy`, `ReconnectRecoveryStrategy` — unit tested with stubs
- `ByteBufScanner` — unit tested for all utility methods including timestamp edge cases
- `VenueRegistry` — tested with a stub `ConnectorFactory` registered via ServiceLoader
- `GatewayBootstrap` startup sequence — integration test with in-memory stubs throughout
- In-memory `Publisher` implementation used in all Phase 1 tests

**Phase 1 acceptance criteria** — core infrastructure only, no venue or transport:

- AC-27: `DefaultParseContext` allocated exactly once per connector in `init()`
- AC-28: `resetSession()` renews `sequenceTracker` and `snapshotGatekeeper` in place
- AC-29: `snapshotCtx` and `parseCtx` share the same `SnapshotGatekeeper` after Phase A of recovery
- AC-30: `recoveryCtx` is a new instance after every recovery Phase A
- AC-31: `RecoveryStrategy.execute()` calls exactly one of `onChannelRestored()` / `onRecoveryFailed()`
- AC-31a: `recoveryInProgress` flag spans both Phase A and Phase B
- AC-31b: `recoveryCompletions` counter increments only after `onSnapshotBoundaryAccepted()` fires
- AC-32: `ConnectorContext` not held as a field after `init()` returns
- AC-33: Hot-path pipeline produces no per-message heap allocation (stub publisher, 1,000,000 messages)
- AC-33a: Synchronous publisher does not retain the passed `DirectBuffer`
- AC-34: Encode buffer reuse count equals total published message count
- AC-35: No per-message encode buffer allocation
- AC-36: Messages exceeding 10,000 entries rejected without extra allocation
- AC-36a: `ByteBufScanner.parseDecimal()` produces exact `(mantissa, scale)` without floating-point parsing
- AC-38: RFC 3339 timestamp parsing converts to epoch nanoseconds without allocating `java.time` objects
- AC-53: Encoded messages are little-endian with the correct 47-byte body layout
- AC-54: Decoder uses `blockLength` to locate repeating group
- AC-55: Epoch-zero timestamps encoded as `-1`
- AC-61: No `core/` → `venue/` imports (ArchUnit — verified against the stub venue package)
- AC-63: `VenueRegistry` uses ServiceLoader exclusively
- AC-64 through AC-68: Config validation (empty instruments, duplicate IDs, missing credentials)

---

### Phase 2 — Coinbase L2 Connector (live feed, full recovery deferred)

**Goal:** A working `COINBASE_L2` connector that connects, authenticates,
subscribes, parses, encodes, and publishes L2 book data from a live or mock
Coinbase Exchange Direct Feed. Recovery scaffolding exists, but full recovery
behavior is deferred to Phase 3.

Deliverables:
- `CoinbaseL2FeedParser` — snapshot, l2update, heartbeat, subscriptions ack
- `CoinbaseL2SubscriptionBuilder` — full HMAC auth payload construction
- `CoinbaseL2SnapshotStrategy` — `SUBSCRIBE_DRIVEN`; gate opens on snapshot; calls `onSnapshotBoundaryAccepted()`
- `CoinbaseL2Connector`, `CoinbaseL2ConnectorFactory`
- Netty WebSocket transport with epoll on Linux, BoringSSL TLS
- Metric counters infrastructure needed by the live connector path
- Per-instrument counters and active WebSocket connection gauge
- Heartbeat liveness timeout detection (detection only; no recovery action in Phase 2)
- `CoinbaseL2RecoveryStrategy` — stub that calls `onChannelRestored()` immediately (recovery logic in Phase 3)
- Fixture-driven parser tests against all Coinbase L2 fixture files

**Phase 2 acceptance criteria** — parser, encoding, sequencing, transport, and
metric counters only; recovery, metrics REST endpoint, and stream-integrity ACs
deferred to Phase 3:

- AC-1: Gateway connects, authenticates, and subscribes via Coinbase WebSocket
- AC-2: Inbound snapshot parsed, encoded, and published with correct field values
- AC-3: Inbound l2update parsed and published with correct action mapping
- AC-4: Heartbeat updates health state and counters; does not produce SBE message
- AC-5: Unknown message types ignored and counted
- AC-6: `product_id` mismatch rejected and counted
- AC-7: Unresolvable symbols dropped and counted
- AC-8: `gatewayMessageSeq` monotonically increasing from 1 per session
- AC-9: `subscriptions` ack validated; mismatch counted and logged
- AC-10: Authenticated subscribe sent for `level2` and `heartbeat` after connect
- AC-12: Snapshot boundary tracked; stream state internally consistent
- AC-15: Pre-snapshot updates dropped, not buffered
- AC-16: First accepted snapshot establishes the book boundary
- AC-17: `seq1 = seq2 = gatewayMessageSeq` for every Coinbase L2 message
- AC-18: `sequenceTracker().current()` = N for the Nth published message
- AC-41, AC-44, AC-45: Metric counters allocated and updated for the live connector path
- AC-48: Netty epoll + BoringSSL on Linux
- AC-52: Time-dependent behaviour testable via injectable clocks
- AC-53, AC-54, AC-55, AC-56: `COINBASE_L2` wire format and Coinbase L2 timestamp handling
- AC-57 through AC-60: `COINBASE_L2` wire format (bookDepth, templateId, and entry sizes for the `BOOK_LEVEL` template)
- AC-62: `COINBASE_L2` sub-package contains all required implementations

---

### Phase 3 — Recovery and Operational Hardening

**Goal:** Full recovery path. Stream-integrity detection. Graceful shutdown.
Backpressure handling. All acceptance criteria passing.

Deliverables:
- `CoinbaseL2RecoveryStrategy` — complete reconnect/resubscribe/await-snapshot path
- Self-triggered recovery: sequence gaps, stream-integrity failure, heartbeat timeout
- Control-plane recovery on invalid `subscriptions` acknowledgement
- Downstream recovery request handling: venue + instrumentId routing, coalescing
- Backpressure-triggered recovery
- Graceful shutdown on `SIGTERM` with `shutdownDeadlineMs` enforcement
- Publisher-stage and ingress-to-handoff latency counters
- Prometheus `/metrics` endpoint via generic `CountersReader` iteration
- Automatic exposure of newly allocated counters through the metrics REST endpoint
- Agrona `DistinctErrorLog` for hot-path error recording
- Validation that memory-mapped counter values remain readable after abnormal JVM termination
- CPU affinity support (config-driven, optional)

**Phase 3 acceptance criteria** — all remaining ACs:

- AC-9a: Invalid `subscriptions` ack triggers recovery
- AC-11: Authenticated unsubscribe sent before connection close during recovery and graceful shutdown
- AC-13, AC-14: Autonomous recovery on stream-integrity failure
- AC-19 through AC-26: Full recovery ordering and two-phase lifecycle
- AC-31a, AC-31b: Recovery flag and counter timing
- AC-33b, AC-36b, AC-36c, AC-37, AC-39, AC-40, AC-40a: Allocation, latency, and observability
- AC-42, AC-43, AC-46, AC-47: Metrics REST endpoint, discovery, persistence validation, and error logging
- AC-49 through AC-51: Graceful shutdown, heartbeat recovery, reconnect backoff

---

### Phase 4 — Second Venue Strategy

**Goal:** Prove extensibility by adding `COINBASE_L3` as the second venue
strategy with zero changes to `core/`. This validates that base classes
eliminate boilerplate, ServiceLoader wiring works end-to-end for a new venue
strategy, and the `ORDER_ENTRY` SBE template is supported alongside
`COINBASE_L2`.

**Phase 4 acceptance criteria:**
- All Phase 1–3 criteria continue to pass for `COINBASE_L2`
- `COINBASE_L3` passes fixture-driven parser and integration tests
- AC-36d: L3 `BOOK_RESET` preserves venue template semantics
- AC-57 through AC-60: correct `bookDepth`, `templateId`, and entry sizes for the `ORDER_ENTRY` template
- AC-62: `venue/coinbase/l3/` sub-package is complete
- No `core/` class was modified to add the new venue (verified by diff)

---

## 27. Final Notes

This system is designed to be:

- **Production-realistic** — not a toy; designed for live market data at exchange speed
- **Performance-conscious** — off-heap buffers, no hot-path allocation, deterministic execution
- **Extensible** — new venues added in isolation; core never changes
- **Transparent** — no hidden framework behavior; every allocation and ownership decision is explicit
- **Testable** — injectable clocks, in-memory publishers, fixture-driven parser tests

It does **not** claim:
- True zero-copy across TLS / WebSocket / JSON
- Zero allocation (control path allocates freely)
- Reuse of the exact same inbound Netty `ByteBuf` object instance for every message

It **does** achieve:
- Direct parsing from off-heap Netty buffers with no heap copy of JSON payloads
- Direct normalization into SBE-style binary encoding in a single pass
- Reusable off-heap Agrona encode buffers with no per-message allocation on the hot path
- Deterministic, cache-friendly, event-loop-affine execution
- Explicit downstream-to-gateway recovery signaling with clean session boundary semantics
- A clean per-venue package structure where adding an exchange means adding a package,
  not modifying the core
