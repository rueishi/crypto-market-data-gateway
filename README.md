# Crypto Market Data Gateway

A Java 21 market data gateway for low-latency ingestion, normalization, and downstream publishing of crypto exchange feeds. Each process instance is configured for a single venue and maintains one isolated WebSocket connection per instrument, giving each stream clear ownership of parsing, sequencing, recovery, and metrics. 

The current version ships with Coinbase Exchange L2 and Coinbase Exchange Direct L3 over WebSocket. The L2 path validates subscriptions, parses subscribe-driven book snapshots and `l2update` deltas, encodes normalized price-level events as compact SBE-style binary messages, and publishes them through a configurable downstream publisher. The L3 path supports the authenticated Coinbase Exchange Direct full-order feed, including REST-then-delta snapshot recovery, order lifecycle messages, and trade events. Downstream systems can also send SBE-style recovery control messages to request recovery for a single instrument or a batch of instruments.

The project is intentionally small and explicit: Netty for transport, Agrona for counters and direct buffers, handwritten JSON scanning for venue frames, handwritten SBE-style binary encoding for downstream output and recovery control input, a plugin-in style **ServiceLoader-backed venue registry**, and deterministic tests around the parser, connector lifecycle, recovery, observability, and publisher contracts.

## Philosophy

Do one thing, and do that one thing perfectly: move one configured venue's market data into a deterministic, low-latency normalized stream. The design favors small ownership boundaries, explicit state, single-writer hot paths, one connection per instrument, and predictable failure domains over broad multiplexing or framework magic.

## Current Scope

Implemented:

- Coinbase Exchange L2 connector for `level2` and `heartbeat` channels.
- Coinbase Exchange Direct L3 connector for the authenticated `full` and `heartbeat` channels.
- Platform core contracts for venue plugins: connector factory, connector lifecycle, feed parser, subscription builder, snapshot strategy, and recovery strategy.
- Configurable venue selection via `venue: <VENUE_ENUM>` in YAML.
- One-venue-per-instance runtime topology by design; run multiple gateway processes for multiple venues.
- One WebSocket connection per configured instrument for failure isolation, sequence isolation, and single-writer hot-path ownership.
- Coinbase WebSocket authentication with Base64-decoded API secret and HMAC-SHA256 over `timestamp + "GET" + "/users/self/verify"`.
- Subscribe-driven snapshot gating: updates are ignored until the first valid snapshot opens the stream.
- REST-then-delta snapshot alignment for Coinbase Exchange Direct L3.
- Handwritten SBE-style encoding for L2 `BOOK_LEVEL`, L3 `ORDER_EVENT`, and L3 `TRADE_EVENT` payloads.
- Connector-owned recovery execution with reconnect, resubscribe, reset publication, snapshot-boundary completion, autonomous venue-triggered recovery, and downstream-triggered recovery requests.
- Dedicated downstream-to-gateway SBE recovery control codecs for single-instrument `RECOVERY_REQUEST` and multi-instrument `RECOVERY_REQUEST_BATCH`.
- Agrona-backed mapped counters and distinct error log.
- Prometheus-format `/metrics` endpoint.
- `IN_MEMORY` publisher for deterministic tests and local capture.
- `LOGGING` publisher backed by Log4j 2 async loggers and LMAX Disruptor.
- Maven and Gradle builds, including Gradle Wrapper for machines without Gradle installed.

Intentionally left open:

- Production downstream publishing is a platform integration boundary. Teams can plug in the infrastructure that matches where their order-book engine sits, such as shared memory for co-located processes, Aeron for low-latency IPC/UDP publication, Chronicle Queue for durable replay, or another internal bus.

Next venue and depth work:

- Add Binance as a new venue plugin.
- Add Kraken as a new venue plugin.

## Architecture
![architecture](https://github.com/user-attachments/assets/fbc294b1-b5a2-460b-bfc4-80c996176463)

```text
Coinbase WebSocket / Coinbase Exchange Direct WebSocket
        |
        v
NettyWebSocketTransport
        |
        v
CoinbaseL2Connector or CoinbaseL3Connector
        |
        v
CoinbaseL2FeedParser or CoinbaseL3FeedParser
        |
        v
SbeEncoder / trade encoder -> Publisher
        |
        +--> InMemoryPublisher
        +--> LoggingPublisher -> Log4j 2 AsyncLoggerContext -> rolling file

Downstream recovery control:
  DirectBuffer RECOVERY_REQUEST / RECOVERY_REQUEST_BATCH
        |
        v
  SbeRecoveryRequestReceiver
        |
        v
  GatewayRuntime.requestRecovery -> RecoveryRequestRouter -> owning Connector

Observability:
  GatewayCounters / InstrumentCounters -> Agrona CountersManager
  MetricsEndpoint -> GET /metrics
  GatewayErrorLog -> mapped distinct error log
  Recovery execution -> connector-owned single-thread recovery path
```

The hot path avoids a normalized object model. Parser code scans inbound JSON frames and writes directly into the reusable encoder buffer. The publisher contract controls ownership at the handoff boundary: a publisher must consume or copy the buffer region before returning, because the encoder reuses the buffer for the next message.

## Platform Model

This repository is intended to be a gateway platform core, not a single-exchange application hardwired to Coinbase. The core owns the runtime lifecycle, configuration validation, transport contracts, encoding, publishing, recovery orchestration, counters, error logging, and metrics. Exchange-specific behavior lives behind venue plugins.

A venue plugin is responsible for:

- declaring a `ConnectorFactory` discoverable by `ServiceLoader`
- translating a typed venue config block into connector-local components
- building authenticated subscribe/unsubscribe payloads
- parsing venue frames into the shared binary encoder
- defining snapshot and recovery behavior for that venue

The runtime selects exactly one venue per process from YAML:

```yaml
venue: COINBASE_L2
```

That one-venue-per-instance shape is intentional. It keeps failure domains, credentials, liveness, recovery, CPU/event-loop ownership, and operational rollout boundaries clean. If you need Coinbase and Binance at the same time, run two gateway instances with two configs rather than one process with mixed venue state.

Within that venue instance, you can configure one or more instruments:

```yaml
instruments:
  - exchangeSymbol: BTC-USD
    instrumentId: 1001
  - exchangeSymbol: ETH-USD
    instrumentId: 1002
```

Each instrument gets its own connector lifecycle, WebSocket connection, event-loop ownership, sequence state, encode buffer, and per-instrument counters while sharing the process-level publisher and observability runtime. This is not Coinbase-specific; it is the platform rule. If an exchange natively multiplexes symbols on one connection, that multiplexing must be hidden inside the venue implementation without changing the core one-connection-per-instrument model.

## Repository Layout

```text
config/
  coinbase-l2-direct.yaml        Runtime config example for Coinbase L2

scripts/
  run-coinbase-l2-direct.ps1     Windows helper for the Coinbase L2 runtime example

src/main/java/io/rueishi/marketdata/crypto/
  core/
    bootstrap/                   Runtime bootstrap, venue registry, connector wiring
    config/                      YAML-bound config models and validation
    connector/                   Connector lifecycle and recovery orchestration
    encoding/                    SBE-style binary encoder and wire constants
    observability/               Agrona counters, error log, metrics endpoint
    parser/                      Parser context and allocation-conscious scanners
    publisher/                   Publisher interface and implementations
    recovery/                    Recovery request routing, strategies, and SBE control receiver/codecs
    sequence/                    Sequence tracking
    snapshot/                    Snapshot gating and snapshot context
    subscription/                Subscription builder contract
    transport/                   Netty WebSocket transport
  venue/coinbase/
    shared/                      Coinbase auth and typed venue config
    l2/                          Coinbase L2 connector, parser, subscription builder, recovery
    l3/                          Coinbase Exchange Direct L3 connector, parser, snapshot, subscription builder, recovery

src/test/
  java/                          Unit and integration tests
  resources/venue/coinbase/      Coinbase L2 and L3 JSON fixtures
```

## Requirements

- JDK 21
- PowerShell for `scripts/run-coinbase-l2-direct.ps1`
- Coinbase Exchange API credentials for live/sandbox Coinbase runs
- Maven or the checked-in Gradle Wrapper

The Gradle Wrapper downloads Gradle automatically. Developers do not need a global Gradle installation.

## Build And Test

### Maven

```powershell
mvn test
```

Package without tests:

```powershell
mvn -DskipTests package
```

### Gradle Wrapper

Windows:

```powershell
.\gradlew.bat clean test
```

macOS/Linux:

```bash
./gradlew clean test
```

Gradle writes to `build-gradle/` so it stays separate from Maven's `target/` output. Both `build-gradle/` and `target/` are ignored by Git.

## Running Coinbase

The runtime entrypoint is `io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap`. It expects all runtime configuration in a YAML file supplied by:

```text
-Dgateway.config=<path>
```

On Windows, the helper script builds the runtime classpath with Maven and starts the L2 example runtime:

```powershell
$env:COINBASE_EXCHANGE_API_KEY = "<exchange-api-key>"
$env:COINBASE_EXCHANGE_API_SECRET = "<exchange-api-secret>"
$env:COINBASE_EXCHANGE_API_PASSPHRASE = "<exchange-api-passphrase>"

.\scripts\run-coinbase-l2-direct.ps1
```

Use a different YAML file on Windows:

```powershell
.\scripts\run-coinbase-l2-direct.ps1 -ConfigPath config\coinbase-l2-direct.yaml
```

On Linux/macOS, the Bash helper does the same startup path for the L2 example:

```bash
export COINBASE_EXCHANGE_API_KEY="<exchange-api-key>"
export COINBASE_EXCHANGE_API_SECRET="<exchange-api-secret>"
export COINBASE_EXCHANGE_API_PASSPHRASE="<exchange-api-passphrase>"

./scripts/run-coinbase-l2-direct.sh
```

If the execute bit is not present on your checkout, run it through Bash:

```bash
bash scripts/run-coinbase-l2-direct.sh
```

Use a different YAML file on Linux/macOS:

```bash
./scripts/run-coinbase-l2-direct.sh --config config/coinbase-l2-direct.yaml
```

The scripts validate that the three Coinbase environment variables exist, build `target/classes` plus a dependency classpath, and launch `GatewayBootstrap`. They are currently L2 example launchers. Coinbase Exchange Direct L3 runs through the same bootstrap path, but you must supply an L3 YAML config with `venue: COINBASE_L3` and Coinbase channels compatible with the authenticated full-order feed.

### Coinbase Credentials

The Coinbase venue configs use placeholders like:

```yaml
coinbase:
  apiKey: ${COINBASE_EXCHANGE_API_KEY}
  apiSecret: ${COINBASE_EXCHANGE_API_SECRET}
  passphrase: ${COINBASE_EXCHANGE_API_PASSPHRASE}
```

Use Coinbase Exchange API credentials for these paths. Do not use Advanced Trade, CDP, Prime, or other Coinbase API key families unless they are explicitly compatible with the Exchange WebSocket auth scheme. The Exchange secret is expected to be Base64-decodable. Coinbase Exchange Direct L3 uses the same Coinbase Exchange credential family but targets the authenticated full-order feed.

Secrets are resolved at startup and must not be committed into YAML, logs, metrics labels, or exception messages.

### Endpoint Selection

The checked-in L2 example config currently points at Coinbase's sandbox Direct Feed:

```yaml
coinbase:
  endpoint: wss://ws-direct.sandbox.exchange.coinbase.com
```

For production Direct Feed, use:

```yaml
coinbase:
  endpoint: wss://ws-direct.exchange.coinbase.com
```

If you are validating access or diagnosing auth, be careful not to mix sandbox credentials with production endpoints.

## Configuration

The checked-in example config lives at [config/coinbase-l2-direct.yaml](config/coinbase-l2-direct.yaml). It is an L2 example config for the Coinbase Direct WebSocket.

Important sections:

```yaml
instanceId: coinbase-l2-direct-local
environment: dev
venue: COINBASE_L2

instruments:
  - exchangeSymbol: BTC-USD
    instrumentId: 1001

transport:
  connectTimeoutMs: 5000
  heartbeatTimeoutMs: 30000
  frameSizeLimitBytes: 65536

encoding:
  maxLevelsPerMessage: 10000
  bufferHeadroomBytes: 0

observability:
  countersSharedMemoryPath: var/run/coinbase-l2-direct/counters
  errorLogPath: var/run/coinbase-l2-direct/error.log
  metricsHttpPort: 0

publisher:
  type: LOGGING
  logging:
    outputPath: var/run/coinbase-l2-direct/messages.log
    rollSizeMb: 256
```

`venue` selects the exchange/depth plugin for this process, such as `COINBASE_L2` or `COINBASE_L3`. The runtime initializes connectors only for that venue; one process should not mix venues.

For Coinbase Exchange Direct L3, the same `coinbase:` config block is used, but the venue-specific subscription set must match the L3 connector. In practice that means:

- `venue: COINBASE_L3`
- Direct Feed endpoint under `coinbase.endpoint`
- authenticated Coinbase Exchange credentials
- `coinbase.channels` including `full` and `heartbeat`

`instrumentId` is the stable internal numeric id encoded into downstream binary messages. Add products for the configured venue by adding `instruments` entries with unique ids. Each configured instrument creates a distinct connector and WebSocket connection.

`metricsHttpPort: 0` asks the OS for an ephemeral metrics port. Set a fixed port, such as `9090`, for scraping.

## Publishers

The publisher layer is intentionally a boundary, not a prescribed infrastructure choice. The gateway normalizes and encodes market data; the downstream handoff should adapt to where the consuming order-book engine runs. A deployment might choose shared memory for a co-located engine, Aeron for low-latency IPC or network publication, Chronicle Queue for durable replay, or a proprietary transport. The core contract stays the same so those choices do not leak into venue parsing or recovery logic.

All publishers implement:

```java
boolean publish(DirectBuffer buffer, int offset, int length,
                InstrumentCounters counters, NanoClock nanoClock);

void publishReset(int instrumentId, byte venueByte,
                  byte bookDepthByte, byte templateIdByte,
                  NanoClock nanoClock);
```

### IN_MEMORY

`InMemoryPublisher` copies published messages into a bounded in-memory ring. It is used heavily in tests because callers can read `lastMessage()` and `allMessages()` to assert exact SBE output.

Use it for deterministic local experiments and tests:

```yaml
publisher:
  type: IN_MEMORY
```

### LOGGING

`LoggingPublisher` is an integration-validation sink. It copies the encoded SBE bytes, Base64-encodes the copy, and submits the line to a dedicated Log4j 2 async logger context backed by LMAX Disruptor. The active file rolls by size.

Example:

```yaml
publisher:
  type: LOGGING
  logging:
    outputPath: var/run/coinbase-l2-direct/messages.log
    rollSizeMb: 256
```

Log line format:

```text
sbe_base64=<base64-encoded-sbe-message>
```

`LoggingPublisher` is intentionally not part of the zero-allocation hot-path acceptance criteria. It is useful for validating the end-to-end path and preserving an exact local trace of encoded messages.

## Wire Format

Messages use a handwritten SBE-style binary layout. Market-data messages flow from gateway to downstream through `SbeEncoder`. Recovery control messages flow in the opposite direction through `SbeRecoveryRequestEncoder`, `SbeRecoveryRequestDecoder`, and `SbeRecoveryRequestReceiver`.

Market-data header:

| Field | Offset | Size | Notes |
|---|---:|---:|---|
| `magic` | 0 | 2 | `0xEB0B` |
| `version` | 2 | 1 | current version `1` |
| `templateId` | 3 | 1 | `BOOK_LEVEL=1`, `ORDER_EVENT=2`, `TRADE_EVENT=3` |
| `blockLength` | 4 | 2 | fixed body length |
| `entryCount` | 6 | 2 | repeating-group entry count |

Market-data body:

| Field | Offset | Notes |
|---|---:|---|
| `eventType` | 8 | `BOOK_RESET=1`, `BOOK_SNAPSHOT=2`, `BOOK_UPDATE=3`, `BOOK_TRADE=4` |
| `venue` | 9 | `COINBASE_L2=1`, `COINBASE_L3=2` in the current implementation |
| `bookDepth` | 10 | encoded venue depth |
| `instrumentId` | 11 | internal id from YAML |
| `gatewayMessageSeq` | 15 | gateway sequence |
| `seq1` | 23 | venue sequence or placeholder |
| `seq2` | 31 | venue sequence or placeholder |
| `exchangeTimestamp` | 39 | exchange timestamp nanos or `-1` |
| `ingressTimestamp` | 47 | gateway ingress timestamp nanos |

Repeating groups start at offset `55`.

`BOOK_LEVEL` entries are 20 bytes each:

- side
- action
- reserved bytes
- price mantissa
- quantity mantissa
- price scale
- quantity scale

`ORDER_EVENT` entries are 55 bytes each and encode L3 order lifecycle data such as order id, side, action, reason, order type, timestamp, price, quantity, and old quantity.

`TRADE_EVENT` entries are 51 bytes each and encode L3 trade data such as maker/taker order ids, taker side, price, and quantity.

See [EncodingConstants.java](src/main/java/io/rueishi/marketdata/crypto/core/encoding/EncodingConstants.java) and [SbeEncoder.java](src/main/java/io/rueishi/marketdata/crypto/core/encoding/SbeEncoder.java) for the exact offsets and validation rules.

### Recovery Control Messages

Downstream-triggered recovery uses separate control-plane templates. Downstream sends these messages to the gateway; it does not send `BOOK_RESET` back to the gateway. `BOOK_RESET` remains a gateway-to-downstream market-data event that is published only after recovery is accepted. Recovery work is serialized per connector on a connector-owned recovery path so reconnect or handshake operations do not block Netty transport callbacks.

Recovery control header:

| Field | Offset | Size | Notes |
|---|---:|---:|---|
| `magic` | 0 | 2 | `0xEB0B` |
| `version` | 2 | 1 | current version `1` |
| `templateId` | 3 | 1 | `RECOVERY_REQUEST=101`, `RECOVERY_REQUEST_BATCH=102` |
| `blockLength` | 4 | 2 | fixed body length for the selected recovery template |
| `entryCount` | 6 | 2 | `0` for single request, number of instrument ids for batch |

`RECOVERY_REQUEST` body:

| Field | Relative Offset | Size | Notes |
|---|---:|---:|---|
| `venue` | 0 | 1 | target venue byte, such as `COINBASE_L2=1` or `COINBASE_L3=2` |
| `requestType` | 1 | 1 | `RESET=1`, `RESNAPSHOT=2`, `RESYNC=3` |
| `reasonCode` | 2 | 1 | recovery reason code |
| `reserved` | 3 | 1 | currently `0` |
| `instrumentId` | 4 | 4 | internal id from config |
| `requestTimestamp` | 8 | 8 | request timestamp |
| `diagnosticLength` | 16 | 2 | optional UTF-8 diagnostic byte length |
| `diagnosticText` | 18 | variable | optional UTF-8 diagnostic text |

`RECOVERY_REQUEST_BATCH` body:

| Field | Relative Offset | Size | Notes |
|---|---:|---:|---|
| `venue` | 0 | 1 | target venue byte |
| `requestType` | 1 | 1 | shared recovery action for every instrument |
| `reasonCode` | 2 | 1 | shared recovery reason |
| `reserved` | 3 | 1 | currently `0` |
| `requestTimestamp` | 4 | 8 | request timestamp |
| `diagnosticLength` | 12 | 2 | optional UTF-8 diagnostic byte length |
| `diagnosticText` | 14 | variable | optional UTF-8 diagnostic text |
| `instrumentIds` | after diagnostic text | `4 * entryCount` | repeating group of internal instrument ids |

The decoder expands a batch into one internal `RecoveryRequest` per instrument. Malformed control messages, unsupported template ids, invalid enum values, empty batches, truncated payloads, and market-data `BOOK_RESET` messages are rejected before routing.

## Observability

The runtime creates mapped Agrona counters and a mapped distinct error log from the YAML paths.

Metrics:

- exposed on `GET /metrics`
- formatted in Prometheus text format
- served by a dedicated JDK HTTP server thread
- discovered generically from the shared `CountersReader`

Error log:

- stored at `observability.errorLogPath`
- based on Agrona `DistinctErrorLog`
- intended for repeated parser, transport, recovery, and shutdown failures without log spam

Message trace:

- if `publisher.type: LOGGING`, encoded payloads are written to `publisher.logging.outputPath`
- each line is `sbe_base64=...`

## Recovery And Liveness

Coinbase L2 is subscribe-driven, while Coinbase Exchange Direct L3 uses REST-then-delta recovery for the authenticated full-order feed:

1. Connect WebSocket.
2. Send authenticated subscribe payload.
3. Wait for subscription acknowledgement.
4. Ignore order-book updates until a valid snapshot arrives.
5. Open the update gate after the snapshot boundary.
6. Track heartbeat liveness through counters.
7. On recovery, publish a reset, reconnect/resubscribe on the connector-owned recovery path, and wait for a fresh snapshot boundary.

Recovery can be triggered internally by parser, liveness, or publisher failure paths, or externally by a downstream control message. The downstream path is:

1. A downstream adapter receives a binary `RECOVERY_REQUEST` or `RECOVERY_REQUEST_BATCH`.
2. The adapter passes the caller-owned buffer region to `GatewayRuntime.recoveryRequestReceiver().receive(...)`.
3. `SbeRecoveryRequestReceiver` decodes and validates the message with `SbeRecoveryRequestDecoder`.
4. Each decoded request is delegated to `GatewayRuntime.requestRecovery(...)`.
5. `RecoveryRequestRouter` matches by venue and instrument id and routes the request to the owning connector.
6. If recovery is accepted, the gateway publishes `BOOK_RESET`, reconnects/resubscribes, and waits for the next valid snapshot boundary.

The recovery code is intentionally shared through core contracts so future venues can reuse the same lifecycle shape. Duplicate requests while recovery is already in progress are coalesced by the connector path rather than starting duplicate reconnects or duplicate reset publications.

## Development Notes

- Use `rg` for navigation; the repository is compact and heavily tested.
- Treat `core/` as the platform boundary. New exchange support should enter as a venue package and `ConnectorFactory`, not by special-casing bootstrap.
- Keep one venue per runtime instance. Multi-venue deployments should compose multiple processes.
- Keep one WebSocket connection per configured instrument. Do not introduce shared cross-instrument connection state into core.
- Keep venue code stateless; session state belongs in connector/context objects.
- Do not log secrets. Credential values should never appear in diagnostics.
- Do not call Log4j directly from parsers, connectors, or encoders. Use counters, `GatewayErrorLog`, or the `Publisher` interface.
- Preserve the publisher buffer-ownership contract. The encoder's `DirectBuffer` is reusable and invalid after `publish()` returns.
- Keep Maven and Gradle dependency versions aligned when adding dependencies.

## Useful Commands

Run all Maven tests:

```powershell
mvn test
```

Run all Gradle Wrapper tests:

```powershell
.\gradlew.bat clean test
```

Run a focused Maven test:

```powershell
mvn test "-Dtest=LoggingPublisherTest"
```

Run a focused Gradle test:

```powershell
.\gradlew.bat test --tests "*LoggingPublisherTest"
```

Run Coinbase L2 example:

```powershell
.\scripts\run-coinbase-l2-direct.ps1
```

Run Coinbase L2 example on Linux/macOS:

```bash
./scripts/run-coinbase-l2-direct.sh
```

Inspect generated logging-publisher messages:

```powershell
Get-Content var/run/coinbase-l2-direct/messages.log -Tail 20
```

## Build Outputs

Ignored generated outputs:

- `target/` from Maven
- `build-gradle/` from Gradle
- `build/` from earlier/default Gradle output
- `.gradle/` local Gradle state

## Status

This is an active implementation of a platform core with working Coinbase L2 and Coinbase Exchange Direct L3 venue plugins, production-oriented core contracts, and test-heavy development scaffolding. The current downstream adapters are intended for test capture and integration validation. Production publishing is deliberately left as an integration point for the developer's infrastructure of choice: shared memory, Aeron, Chronicle Queue, or another downstream path near the order-book engine.

Near-term roadmap:

- Add Binance as a venue plugin.
- Add Kraken as a venue plugin.
