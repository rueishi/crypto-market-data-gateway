package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.AbstractConnector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import io.rueishi.marketdata.crypto.core.transport.NettyWebSocketTransport;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.agrona.concurrent.CachedNanoClock;

/**
 * Production connector for one Coinbase L3 full-order feed instrument stream.
 *
 * <p>{@code CoinbaseL3Connector} is the thin venue-specific subclass of
 * {@link AbstractConnector} for the Coinbase full-order feed. The shared base
 * owns parser delegation, schema v2 encoding, snapshot/recovery contexts,
 * recovery coalescing, and lifecycle state. This class supplies the fixed
 * {@link VenueEnum#COINBASE_L3} identity, encoder capacity settings, a
 * dedicated trade encoder for {@link TemplateId#TRADE_EVENT} messages, Netty
 * WebSocket transport wiring from {@link CoinbaseConfig}, and heartbeat
 * liveness timeout detection that triggers autonomous reset recovery when the
 * exchange stops delivering heartbeats within the configured window.</p>
 *
 * <p>Phase 4 Track B factory connector uses a deferred snapshot strategy so
 * that the {@link CoinbaseL3SnapshotStrategy} (which requires a live
 * {@link io.rueishi.marketdata.crypto.core.parser.ParseContext} and event-loop
 * executor) can be wired in {@link #onInitialized(ConnectorContext)} rather
 * than at factory time. Tests pass an explicit {@link SnapshotStrategy} through
 * the test constructor to keep unit tests independent of HTTP infrastructure.</p>
 *
 * <p>The deferred factory path also wires the {@link NettyWebSocketTransport}
 * in {@link #onInitialized(ConnectorContext)} after the event loop and transport
 * config become available through the {@link ConnectorContext}. The test
 * constructor path skips transport wiring; transport hooks become no-ops when
 * {@code transport} is null, preserving backward compatibility with unit tests
 * that do not drive a live WebSocket.</p>
 *
 * <p>{@link CoinbaseL3ConnectorFactory} typically instantiates this class via
 * the deferred factory constructor. Bootstrap then calls
 * {@link #init(ConnectorContext)}, and later {@link #connect()} opens the
 * Netty WebSocket connection before the base class sends the authenticated
 * subscribe payload. An optional {@code restApiBase} entry in the raw venue
 * config map allows integration tests to redirect REST snapshot requests to a
 * local mock HTTP server.</p>
 */
public final class CoinbaseL3Connector extends AbstractConnector {

    private static final int MAX_ENTRY_COUNT = 10_000;
    private static final int HEADROOM_BYTES = 8_192;

    // Deferred snapshot wiring (factory path only; null in test path).
    private final CoinbaseAuthenticator auth;
    private final CoinbaseL3FeedParser l3Parser;
    private final DeferredSnapshotHolder deferredSnapshot;

    // Transport (factory path only; null in test path).
    private NettyWebSocketTransport transport;
    private ScheduledFuture<?> livenessTask;
    private boolean connectionGaugeActive;

    // Liveness state (populated in onInitialized when heartbeatTimeoutMs > 0).
    private long heartbeatTimeoutNanos;
    private long livenessPeriodMillis;
    private boolean livenessMonitoringActive;

    /**
     * Creates a connector with all dependencies supplied explicitly.
     *
     * <p>Tests use this constructor to wire a deterministic or no-op
     * {@link SnapshotStrategy} without starting HTTP infrastructure. The connector
     * ignores the deferred snapshot path entirely in this mode, and all transport
     * lifecycle hooks ({@link #onConnect()}, {@link #onSubscribe(byte[])}, etc.)
     * become no-ops since {@code transport} is left null.</p>
     *
     * @param instrument configured instrument served by this connector
     * @param parser Coinbase L3 feed parser
     * @param snapshot snapshot strategy for the L3 feed
     * @param recovery recovery strategy for the L3 feed
     * @param subscription authenticated subscription builder
     * @throws NullPointerException if any argument is null
     */
    public CoinbaseL3Connector(
            InstrumentConfig instrument,
            CoinbaseL3FeedParser parser,
            SnapshotStrategy snapshot,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription) {
        super(instrument, parser, snapshot, recovery, subscription);
        this.auth = null;
        this.l3Parser = null;
        this.deferredSnapshot = null;
    }

    /**
     * Creates a connector for factory use with deferred snapshot and transport initialization.
     *
     * <p>The factory provides authenticator and parser but cannot wire the
     * {@link CoinbaseL3SnapshotStrategy} at this point because the
     * {@link io.rueishi.marketdata.crypto.core.parser.ParseContext} and
     * event-loop executor are not yet available. Both the snapshot strategy and
     * the {@link NettyWebSocketTransport} are created and installed in
     * {@link #onInitialized(ConnectorContext)} once all bootstrap dependencies
     * are available.</p>
     *
     * @param instrument configured instrument served by this connector
     * @param parser Coinbase L3 feed parser
     * @param recovery recovery strategy for the L3 feed
     * @param subscription authenticated subscription builder
     * @param auth Coinbase authenticator for REST and WebSocket signing
     * @throws NullPointerException if any required argument is null
     */
    public CoinbaseL3Connector(
            InstrumentConfig instrument,
            CoinbaseL3FeedParser parser,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription,
            CoinbaseAuthenticator auth) {
        this(instrument, parser, recovery, subscription, auth, new DeferredSnapshotHolder());
    }

    private CoinbaseL3Connector(
            InstrumentConfig instrument,
            CoinbaseL3FeedParser parser,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription,
            CoinbaseAuthenticator auth,
            DeferredSnapshotHolder holder) {
        super(instrument, parser, holder, recovery, subscription);
        this.auth = Objects.requireNonNull(auth, "auth");
        this.l3Parser = Objects.requireNonNull(parser, "parser");
        this.deferredSnapshot = holder;
    }

    /**
     * Returns the fixed Coinbase L3 venue/depth identity.
     *
     * @return {@link VenueEnum#COINBASE_L3}
     */
    @Override
    protected VenueEnum venueEnum() {
        return VenueEnum.COINBASE_L3;
    }

    /**
     * Returns the maximum ORDER_EVENT entries per encoded message.
     *
     * @return 10 000 entries
     */
    @Override
    protected int maxEntryCount() {
        return MAX_ENTRY_COUNT;
    }

    /**
     * Returns the additional encoder buffer headroom.
     *
     * @return 8 192 bytes
     */
    @Override
    protected int headroomBytes() {
        return HEADROOM_BYTES;
    }

    /**
     * Creates a dedicated {@link SbeEncoder} for {@link TemplateId#TRADE_EVENT} messages.
     *
     * <p>Coinbase L3 match messages use template id 3 ({@code TRADE_EVENT}) and carry
     * a different entry layout from the primary {@code ORDER_EVENT} encoder. One trade
     * entry is sufficient since each {@code match} message encodes a single trade.</p>
     *
     * @return trade encoder initialized with {@link TemplateId#TRADE_EVENT}
     */
    @Override
    protected SbeEncoder createTradeEncoder() {
        return new SbeEncoder(
                instrumentId(),
                venueEnum().byteValue(),
                venueEnum().bookDepth().byteValue(),
                TemplateId.TRADE_EVENT.byteValue(),
                1,
                headroomBytes());
    }

    /**
     * Wires the Netty transport, deferred snapshot strategy, and liveness timing.
     *
     * <p>When this connector was created via the factory constructor, this hook:
     * <ol>
     *   <li>Re-parses the endpoint URI from the venue config map.</li>
     *   <li>Creates and stores a {@link NettyWebSocketTransport} wired to the
     *       connector event loop. The transport is not connected here;
     *       {@link #onConnect()} performs the actual network I/O.</li>
     *   <li>Creates a {@link CoinbaseL3SnapshotStrategy} using the now-available
     *       {@link io.rueishi.marketdata.crypto.core.parser.ParseContext} and
     *       event-loop executor. If the raw venue config contains a
     *       {@code restApiBase} key, that URL is used for REST requests instead
     *       of the production Coinbase URL; this enables integration tests to
     *       redirect snapshot fetches to a local mock HTTP server.</li>
     * </ol>
     * For test connectors (explicit snapshot constructor), this wiring is skipped
     * entirely and the test path operates without transport.</p>
     *
     * @param ctx bootstrap dependency carrier containing event loop and transport config
     */
    @Override
    protected void onInitialized(ConnectorContext ctx) {
        if (deferredSnapshot != null) {
            // Re-parse typed config from the raw venue config map to get the endpoint URI.
            URI endpoint = CoinbaseConfig.from(ctx.venueConfig()).endpoint();
            SslContext sslContext = sslContextFor(endpoint);

            NettyWebSocketTransport configuredTransport = new NettyWebSocketTransport(
                    ctx.eventLoopGroup(),
                    endpoint,
                    sslContext,
                    pipeline -> {
                    },
                    ctx.transportConfig().frameSizeLimitBytes,
                    ctx.transportConfig().connectTimeoutMs,
                    this);
            if (ctx.nanoClock() instanceof CachedNanoClock cachedNanoClock) {
                configuredTransport = configuredTransport.withClock(cachedNanoClock);
            }
            this.transport = configuredTransport;

            // Build snapshot strategy; allow integration tests to override the REST base URL
            // via a "restApiBase" entry in the raw venue config map.
            Object restApiBaseObj = ctx.venueConfig().get("restApiBase");
            CoinbaseL3SnapshotStrategy strategy;
            if (restApiBaseObj instanceof String restApiBase) {
                strategy = new CoinbaseL3SnapshotStrategy(
                        auth,
                        l3Parser,
                        parseContext(),
                        ctx.eventLoopGroup().next()::execute,
                        CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY,
                        HttpClient.newBuilder().build(),
                        restApiBase);
            } else {
                strategy = new CoinbaseL3SnapshotStrategy(
                        auth,
                        l3Parser,
                        parseContext(),
                        ctx.eventLoopGroup().next()::execute);
            }
            deferredSnapshot.setDelegate(strategy);
        }

        long timeoutMs = ctx.transportConfig().heartbeatTimeoutMs;
        if (timeoutMs > 0) {
            this.heartbeatTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            this.livenessPeriodMillis = Math.max(1L, timeoutMs / 2L);
        }
    }

    /**
     * Opens the Coinbase L3 WebSocket transport before the subscribe payload is sent.
     *
     * <p>Increments connection-attempt and connection-success gateway counters and
     * marks the connection gauge active. If the transport is null (test path), this
     * hook is a no-op so that unit tests that do not wire transport can still call
     * {@link #connect()} without error.</p>
     *
     * @throws IllegalStateException if the transport connection fails
     */
    @Override
    protected void onConnect() {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport == null) {
            // Test path: no transport wired; skip network connection.
            return;
        }
        gatewayCounters().connectionAttempts().increment();
        try {
            currentTransport.connect();
            gatewayCounters().connectionSuccesses().increment();
            markConnectionOpen();
        } catch (IOException ex) {
            gatewayCounters().connectionFailures().increment();
            throw new IllegalStateException("Unable to connect Coinbase L3 WebSocket", ex);
        }
    }

    /**
     * Sends the authenticated subscribe payload over the WebSocket when connected.
     *
     * <p>The normal initial connect path opens the transport immediately before
     * this hook runs. Recovery resubscribe calls the same helper after reconnect
     * so the replacement channel receives an authenticated subscribe payload
     * before the connector resets session state and waits for a fresh snapshot.</p>
     *
     * <p>If the transport is null or not connected, the send is silently skipped.
     * This preserves backward compatibility with test connectors that do not wire
     * transport but still drive the subscribe lifecycle via
     * {@link io.rueishi.marketdata.crypto.core.connector.AbstractConnector#connect()}.</p>
     *
     * @param payload Coinbase L3 JSON subscribe payload
     */
    @Override
    protected void onSubscribe(byte[] payload) {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null && currentTransport.isConnected()) {
            currentTransport.sendText(payload);
        }
        // Start liveness monitoring after subscribe, since L3 does not send subscribe ack
        if (!livenessMonitoringActive) {
            livenessMonitoringActive = true;
            if (transport != null) {
                livenessTask = transport.eventLoop().scheduleAtFixedRate(
                        this::runLivenessCheckSafely,
                        TimeUnit.NANOSECONDS.toMillis(heartbeatTimeoutNanos),
                        livenessPeriodMillis,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Sends an unsubscribe payload when the transport is still connected.
     *
     * <p>Shutdown may be called after initialization but before connect, and
     * recovery may attempt a best-effort unsubscribe after the channel has
     * already closed. In those cases the method skips the network send while
     * allowing the base connector to continue publishing reset control messages.</p>
     *
     * @param payload Coinbase L3 JSON unsubscribe payload
     */
    @Override
    protected void onUnsubscribe(byte[] payload) {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null && currentTransport.isConnected()) {
            currentTransport.sendText(payload);
        }
    }

    /**
     * Closes the Coinbase L3 WebSocket transport and stops liveness monitoring during shutdown.
     *
     * <p>Called by the base connector after the unsubscribe payload is emitted.
     * If the transport is null (test path), only the liveness monitoring state
     * is cleaned up.</p>
     */
    @Override
    protected void onShutdown() {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null) {
            currentTransport.close();
        }
        stopLivenessMonitoring();
        markConnectionClosed();
    }

    /**
     * Marks the current session as liveness-monitored after subscription acknowledgement.
     *
     * <p>When the transport is available (factory path), schedules a recurring
     * liveness check at half the heartbeat timeout period on the connector event
     * loop. The first firing is delayed by the full timeout so that the connection
     * has a fair chance to receive the first heartbeat before the check starts.
     * When transport is absent (test path), only the flag is set; tests drive
     * liveness checks directly via {@link #detectLivenessTimeout()}.</p>
     */
    @Override
    protected void onSubscriptionAckValidated() {
        if (livenessMonitoringActive) {
            return;
        }
        livenessMonitoringActive = true;
        if (transport != null) {
            livenessTask = transport.eventLoop().scheduleAtFixedRate(
                    this::runLivenessCheckSafely,
                    TimeUnit.NANOSECONDS.toMillis(heartbeatTimeoutNanos),
                    livenessPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops liveness monitoring and updates active connection state after transport disconnect.
     *
     * <p>Called by the base connector when the transport reports channel loss or close.
     * Shutdown-initiated disconnects therefore still update observability but do not
     * start new recovery.</p>
     *
     * @param cause disconnect cause reported by the transport, or null for a normal close
     */
    @Override
    protected void onTransportDisconnected(Throwable cause) {
        stopLivenessMonitoring();
        markConnectionClosed();
    }

    /**
     * Checks whether the last heartbeat timestamp has exceeded the configured timeout.
     *
     * <p>This method is package-private so integration tests can invoke the
     * timeout logic directly without waiting for the scheduled task. It uses
     * {@link System#nanoTime()} instead of the transport-owned cached clock so
     * liveness still advances when no inbound frames arrive. It is a no-op when
     * shutdown has been requested or liveness monitoring has not started
     * (before subscription acknowledgement).</p>
     *
     * <p>When a timeout is detected: the heartbeatsMissed and livenessFailures counters
     * are incremented, the lastHeartbeatReceivedNanos counter is advanced to the current
     * time to avoid repeated immediate firings, and a {@link RecoveryRequestType#RESET}
     * recovery is requested with {@link RecoveryReasonCode#HEARTBEAT_TIMEOUT}.</p>
     */
    void detectLivenessTimeout() {
        if (shutdownRequested() || !livenessMonitoringActive) {
            return;
        }
        long lastHeartbeatNanos = counters().lastHeartbeatReceivedNanos().get();
        if (lastHeartbeatNanos <= 0) {
            return;
        }
        long nowNanos = System.nanoTime();
        long elapsedNanos = nowNanos - lastHeartbeatNanos;
        if (elapsedNanos >= heartbeatTimeoutNanos) {
            counters().heartbeatsMissed().increment();
            counters().livenessFailures().increment();
            counters().lastHeartbeatReceivedNanos().set(nowNanos);
            requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.HEARTBEAT_TIMEOUT,
                    "Coinbase L3 heartbeat timeout");
        }
    }

    /**
     * Returns the current liveness monitoring state for package-level tests.
     *
     * @return true after subscription acknowledgement starts monitoring
     */
    boolean livenessMonitoringActive() {
        return livenessMonitoringActive;
    }

    /**
     * Returns the per-instrument counter bundle for package-local tests.
     *
     * @return counter bundle cached by the base connector during initialization
     */
    InstrumentCounters instrumentCounters() {
        return counters();
    }

    /**
     * Passes the recovery unsubscribe payload through the base connector's subscription builder.
     *
     * <p>The recovery strategy calls this through {@link DeferredRecoveryActions} so
     * authenticated payload construction stays in the shared base while the factory
     * owns the recovery action wiring.</p>
     */
    void recoveryUnsubscribe() {
        sendUnsubscribe();
    }

    /**
     * Passes the recovery resubscribe payload through the base connector's subscription builder.
     */
    void recoveryResubscribe() {
        sendSubscribe();
    }

    /**
     * Closes the WebSocket channel and stops liveness monitoring as part of recovery.
     *
     * <p>Called by {@link DeferredRecoveryActions#close()} during the recovery
     * sequence (unsubscribe → close → reconnect → resubscribe) before the
     * connector attempts to reconnect. Updates the active-connection gauge and
     * cancels the liveness scheduler so it does not fire during the reconnect gap.</p>
     */
    private void recoveryClose() {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null) {
            currentTransport.close();
        }
        stopLivenessMonitoring();
        markConnectionClosed();
    }

    /**
     * Opens a fresh WebSocket connection during the reconnect step of recovery.
     *
     * <p>Increments the gateway reconnect-attempts counter. On success, marks the
     * connection gauge active. On failure, increments connection-failures and re-throws
     * the {@link IOException} so that {@link CoinbaseL3RecoveryStrategy} can apply
     * reconnect backoff and retry logic.</p>
     *
     * @throws IOException if the transport cannot establish a new connection
     * @throws IllegalStateException if the transport has not been initialized
     */
    private void recoveryReconnect() throws IOException {
        NettyWebSocketTransport currentTransport = requireTransport();
        gatewayCounters().reconnectAttempts().increment();
        try {
            currentTransport.connect();
            markConnectionOpen();
        } catch (IOException ex) {
            gatewayCounters().connectionFailures().increment();
            throw ex;
        }
    }

    /**
     * Returns the transport, throwing if it has not been initialized.
     *
     * @return active transport
     * @throws IllegalStateException if {@code onInitialized} has not been called yet
     */
    private NettyWebSocketTransport requireTransport() {
        if (transport == null) {
            throw new IllegalStateException("Coinbase L3 transport has not been initialized");
        }
        return transport;
    }

    /**
     * Builds a TLS context for {@code wss://} endpoints; returns null for plain {@code ws://}.
     *
     * @param endpoint WebSocket endpoint URI
     * @return TLS context, or null for non-TLS endpoints
     * @throws IllegalStateException if TLS initialization fails
     */
    private static SslContext sslContextFor(URI endpoint) {
        if (!"wss".equalsIgnoreCase(endpoint.getScheme())) {
            return null;
        }
        try {
            return NettyWebSocketTransport.buildClientSslContext();
        } catch (SSLException ex) {
            throw new IllegalStateException("Unable to build Coinbase L3 TLS context", ex);
        }
    }

    /**
     * Increments the active-connections gateway gauge when the connection opens.
     *
     * <p>The guard prevents double-counting if the hook is invoked multiple times
     * for the same channel (e.g., reconnect without intervening close).</p>
     */
    private void markConnectionOpen() {
        if (!connectionGaugeActive) {
            connectionGaugeActive = true;
            gatewayCounters().activeConnections().increment();
        }
    }

    /**
     * Decrements the active-connections gateway gauge when the connection closes.
     *
     * <p>The guard prevents double-decrement if the hook is invoked multiple times
     * for the same channel (e.g., shutdown after disconnect).</p>
     */
    private void markConnectionClosed() {
        if (connectionGaugeActive) {
            connectionGaugeActive = false;
            gatewayCounters().activeConnections().decrement();
        }
    }

    /**
     * Cancels the liveness task and resets the monitoring flag.
     *
     * <p>Called on shutdown, transport disconnect, and at the start of the close
     * step of recovery so that no stale timer fires during reconnect.</p>
     */
    private void stopLivenessMonitoring() {
        livenessMonitoringActive = false;
        ScheduledFuture<?> currentTask = livenessTask;
        if (currentTask != null) {
            currentTask.cancel(false);
            livenessTask = null;
        }
    }

    /**
     * Delegates to {@link #detectLivenessTimeout()} and counts unchecked exceptions as failures.
     *
     * <p>Scheduled directly on the connector event loop so that any unexpected
     * runtime exceptions are counted as liveness failures before propagating to
     * the Netty executor's uncaught-exception handler.</p>
     */
    private void runLivenessCheckSafely() {
        try {
            detectLivenessTimeout();
        } catch (RuntimeException ex) {
            counters().livenessFailures().increment();
            throw ex;
        }
    }

    /**
     * Deferred snapshot holder that forwards to a real strategy once it is available.
     *
     * <p>The factory constructor passes this to {@code super()} so the base-class
     * {@code snapshot} field is satisfied immediately, while the real
     * {@link CoinbaseL3SnapshotStrategy} is installed after initialization.</p>
     */
    static final class DeferredSnapshotHolder implements SnapshotStrategy {
        private volatile CoinbaseL3SnapshotStrategy delegate;

        /**
         * Installs the real snapshot strategy after initialization.
         *
         * @param d fully constructed snapshot strategy to delegate to
         */
        void setDelegate(CoinbaseL3SnapshotStrategy d) {
            this.delegate = d;
        }

        /**
         * Returns {@link Mode#REST_THEN_DELTA} since the real strategy always fetches
         * a REST snapshot before replaying buffered deltas.
         *
         * @return {@link Mode#REST_THEN_DELTA}
         */
        @Override
        public Mode mode() {
            return Mode.REST_THEN_DELTA;
        }

        /**
         * Forwards a snapshot trigger to the real strategy if it has been installed.
         *
         * <p>Calls before the strategy is installed (which cannot happen in the
         * normal lifecycle) are silently dropped.</p>
         *
         * @param instrument configured instrument for which the snapshot is triggered
         * @param ctx snapshot context carrying the gatekeeper and publish helpers
         */
        @Override
        public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
            CoinbaseL3SnapshotStrategy d = delegate;
            if (d != null) {
                d.triggerSnapshot(instrument, ctx);
            }
        }
    }

    /**
     * Recovery actions adapter for factory-wired connectors.
     *
     * <p>The factory creates this adapter before creating the connector, then wires
     * the connector reference after construction — the same pattern used by
     * {@link io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2Connector}.
     * The adapter bridges the {@link CoinbaseL3RecoveryStrategy.Actions} interface
     * to the private transport and channel management methods on this connector.</p>
     */
    static final class DeferredRecoveryActions implements CoinbaseL3RecoveryStrategy.Actions {
        private CoinbaseL3Connector delegate;

        /**
         * Wires the connector after it is constructed.
         *
         * @param connector connector that owns the transport and channel management
         */
        void setDelegate(CoinbaseL3Connector connector) {
            this.delegate = connector;
        }

        /**
         * Sends the authenticated unsubscribe payload through the connector.
         *
         * @throws Exception if the payload cannot be sent
         */
        @Override
        public void unsubscribe() throws Exception {
            requireDelegate().recoveryUnsubscribe();
        }

        /**
         * Closes the WebSocket channel through the connector's transport.
         *
         * @throws Exception if the close step fails
         */
        @Override
        public void close() throws Exception {
            requireDelegate().recoveryClose();
        }

        /**
         * Opens a fresh WebSocket connection through the connector's transport.
         *
         * @throws Exception if the reconnect attempt fails
         */
        @Override
        public void reconnect() throws Exception {
            requireDelegate().recoveryReconnect();
        }

        /**
         * Sends the authenticated resubscribe payload through the connector.
         *
         * @throws Exception if the payload cannot be sent
         */
        @Override
        public void resubscribe() throws Exception {
            requireDelegate().recoveryResubscribe();
        }

        private CoinbaseL3Connector requireDelegate() {
            if (delegate == null) {
                throw new IllegalStateException("Coinbase L3 recovery actions are not attached to a connector");
            }
            return delegate;
        }
    }
}
