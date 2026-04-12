package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.AbstractConnector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import io.rueishi.marketdata.crypto.core.transport.NettyWebSocketTransport;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.agrona.concurrent.CachedNanoClock;

/**
 * Production connector for one Coinbase L2 instrument stream.
 *
 * <p>{@code CoinbaseL2Connector} is the thin venue-specific subclass of
 * {@link AbstractConnector}. The shared base owns parser delegation, encoding,
 * snapshot/recovery contexts, recovery coalescing, and lifecycle state. This
 * class supplies the fixed {@link VenueEnum#COINBASE_L2} identity, encoder
 * capacity settings from startup config, and Coinbase WebSocket transport
 * wiring from {@link CoinbaseConfig}. It also owns the Phase 3 recovery
 * transport actions used by {@link CoinbaseL2RecoveryStrategy} plus the live-path
 * observability that belongs beside transport state: connection gauges,
 * connection outcome counters, and heartbeat timeout detection that now
 * requests the same autonomous recovery path as parser and downstream reset
 * triggers.</p>
 *
 * <p>{@link CoinbaseL2ConnectorFactory} typically instantiates this class once
 * per configured instrument with a {@link CoinbaseL2FeedParser},
 * {@link CoinbaseL2SnapshotStrategy}, {@link CoinbaseL2RecoveryStrategy}, and
 * {@link CoinbaseL2SubscriptionBuilder}. Bootstrap then calls
 * {@link #init(ConnectorContext)}, and later {@link #connect()} opens the
 * Netty WebSocket connection before the base class sends the authenticated
 * subscribe payload.</p>
 */
public final class CoinbaseL2Connector extends AbstractConnector {
    private final URI endpoint;
    private final int maxEntryCount;
    private final int headroomBytes;
    private NettyWebSocketTransport transport;
    private long heartbeatTimeoutNanos;
    private long livenessPeriodMillis;
    private ScheduledFuture<?> livenessTask;
    private boolean connectionGaugeActive;
    private boolean livenessMonitoringActive;

    /**
     * Creates a Coinbase L2 connector with all venue collaborators supplied by the factory.
     *
     * @param instrument configured instrument served by this connector
     * @param parser Coinbase L2 feed parser
     * @param snapshot Coinbase subscribe-driven snapshot strategy
     * @param recovery Coinbase recovery strategy
     * @param subscription Coinbase authenticated subscription builder
     * @param config typed Coinbase venue configuration
     * @param maxEntryCount maximum number of book levels one encoded message can contain
     * @param headroomBytes additional encoder buffer headroom
     * @throws NullPointerException if any required collaborator is null
     * @throws IllegalArgumentException if encoder sizing values are invalid
     */
    public CoinbaseL2Connector(
            InstrumentConfig instrument,
            FeedParser parser,
            SnapshotStrategy snapshot,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription,
            CoinbaseConfig config,
            int maxEntryCount,
            int headroomBytes) {
        super(instrument, parser, snapshot, recovery, subscription);
        Objects.requireNonNull(config, "config");
        if (maxEntryCount <= 0) {
            throw new IllegalArgumentException("maxEntryCount must be positive");
        }
        if (headroomBytes < 0) {
            throw new IllegalArgumentException("headroomBytes must not be negative");
        }
        this.endpoint = config.endpoint();
        this.maxEntryCount = maxEntryCount;
        this.headroomBytes = headroomBytes;
    }

    /**
     * Creates a Coinbase L2 connector with the default full reconnect recovery strategy.
     *
     * <p>The constructor uses a small deferred-actions adapter so the recovery
     * strategy can be supplied to the shared base constructor before this
     * connector instance is fully initialized. After construction, the adapter
     * delegates recovery channel actions back to this connector.</p>
     *
     * @param instrument configured instrument served by this connector
     * @param parser Coinbase L2 feed parser
     * @param snapshot Coinbase subscribe-driven snapshot strategy
     * @param subscription Coinbase authenticated subscription builder
     * @param config typed Coinbase venue configuration
     * @param transportConfig reconnect backoff and transport timing settings
     * @param maxEntryCount maximum number of book levels one encoded message can contain
     * @param headroomBytes additional encoder buffer headroom
     * @throws NullPointerException if any required collaborator is null
     * @throws IllegalArgumentException if encoder sizing or recovery timing values are invalid
     */
    public CoinbaseL2Connector(
            InstrumentConfig instrument,
            FeedParser parser,
            SnapshotStrategy snapshot,
            SubscriptionBuilder subscription,
            CoinbaseConfig config,
            TransportConfig transportConfig,
            int maxEntryCount,
            int headroomBytes) {
        this(
                instrument,
                parser,
                snapshot,
                subscription,
                config,
                transportConfig,
                maxEntryCount,
                headroomBytes,
                new DeferredRecoveryActions());
    }

    private CoinbaseL2Connector(
            InstrumentConfig instrument,
            FeedParser parser,
            SnapshotStrategy snapshot,
            SubscriptionBuilder subscription,
            CoinbaseConfig config,
            TransportConfig transportConfig,
            int maxEntryCount,
            int headroomBytes,
            DeferredRecoveryActions recoveryActions) {
        this(
                instrument,
                parser,
                snapshot,
                new CoinbaseL2RecoveryStrategy(recoveryActions, transportConfig),
                subscription,
                config,
                maxEntryCount,
                headroomBytes);
        recoveryActions.delegate = this;
    }

    /**
     * Returns the fixed Coinbase L2 venue identity used for routing and encoding.
     *
     * @return {@link VenueEnum#COINBASE_L2}
     */
    @Override
    protected VenueEnum venueEnum() {
        return VenueEnum.COINBASE_L2;
    }

    /**
     * Returns the configured Coinbase L2 encoder level capacity.
     *
     * @return maximum repeating-group entry count for BOOK_LEVEL messages
     */
    @Override
    protected int maxEntryCount() {
        return maxEntryCount;
    }

    /**
     * Returns configured Coinbase L2 encoder buffer headroom.
     *
     * @return additional bytes reserved beyond the maximum encoded payload
     */
    @Override
    protected int headroomBytes() {
        return headroomBytes;
    }

    /**
     * Builds the Netty transport after bootstrap dependencies are available.
     *
     * <p>The base connector calls this from {@link #init(ConnectorContext)}
     * after counters, clocks, and contexts are allocated. The transport is
     * created but not connected; {@link #onConnect()} performs network I/O later
     * so bootstrap can initialize all connectors before opening sockets.</p>
     *
     * @param ctx bootstrap dependency carrier containing event loop and transport config
     * @throws NullPointerException if {@code ctx} is null
     * @throws IllegalStateException if TLS setup fails for a secure endpoint
     */
    @Override
    protected void onInitialized(ConnectorContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        SslContext sslContext = sslContextFor(endpoint);
        long heartbeatTimeoutMs = ctx.transportConfig().heartbeatTimeoutMs;
        if (heartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException("transport.heartbeatTimeoutMs must be positive");
        }
        this.heartbeatTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMs);
        this.livenessPeriodMillis = Math.max(1L, heartbeatTimeoutMs / 2L);
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
    }

    /**
     * Opens the Coinbase WebSocket transport before the subscribe payload is sent.
     *
     * @throws IllegalStateException if the transport was not initialized or connection fails
     */
    @Override
    protected void onConnect() {
        NettyWebSocketTransport currentTransport = requireTransport();
        gatewayCounters().connectionAttempts().increment();
        try {
            currentTransport.connect();
            gatewayCounters().connectionSuccesses().increment();
            markConnectionOpen();
        } catch (IOException ex) {
            gatewayCounters().connectionFailures().increment();
            throw new IllegalStateException("Unable to connect Coinbase L2 WebSocket", ex);
        }
    }

    /**
     * Sends the authenticated subscribe payload when the WebSocket is connected.
     *
     * <p>The normal initial connect path opens the transport immediately before
     * this hook runs. Phase 3 recovery uses the same helper after reconnect so
     * the replacement channel receives an authenticated subscribe payload
     * before the connector resets session state and waits for a fresh
     * subscribe-driven snapshot boundary.</p>
     *
     * @param payload Coinbase subscribe JSON payload
     */
    @Override
    protected void onSubscribe(byte[] payload) {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null && currentTransport.isConnected()) {
            currentTransport.sendText(payload);
        }
    }

    /**
     * Sends an unsubscribe payload when the transport is still connected.
     *
     * <p>Shutdown may be called after initialization but before connect, and
     * recovery may attempt a best-effort unsubscribe after the channel has
     * already closed. In those cases there is no active channel to write to, so
     * the method skips the network send while allowing the base connector to
     * continue publishing reset control messages.</p>
     *
     * @param payload Coinbase unsubscribe JSON payload
     */
    @Override
    protected void onUnsubscribe(byte[] payload) {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null && currentTransport.isConnected()) {
            currentTransport.sendText(payload);
        }
    }

    /**
     * Closes the Coinbase WebSocket channel during connector shutdown.
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
     * Starts heartbeat liveness detection after Coinbase validates the subscription ack.
     *
     * <p>The parser seeds {@code lastHeartbeatReceivedNanos} before invoking
     * this hook, so the first timeout window starts at the confirmed
     * subscription boundary instead of transport connect time. Timeout detection
     * requests connector-owned recovery; the base recovery coalescing guard
     * suppresses repeated timer firings while reset is already active.</p>
     */
    @Override
    protected void onSubscriptionAckValidated() {
        if (livenessMonitoringActive) {
            return;
        }
        livenessMonitoringActive = true;
        livenessTask = requireTransport().eventLoop().scheduleAtFixedRate(
                this::runLivenessCheckSafely,
                TimeUnit.NANOSECONDS.toMillis(heartbeatTimeoutNanos),
                livenessPeriodMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Stops liveness monitoring and updates active connection state after transport close.
     *
     * @param cause disconnect cause reported by the transport, or null for normal close
     */
    @Override
    protected void onTransportDisconnected(Throwable cause) {
        stopLivenessMonitoring();
        markConnectionClosed();
    }

    /**
     * Runs one heartbeat timeout detection pass for the current session.
     *
     * <p>This method is package-private so tests can drive detection with an
     * injected clock without waiting for wall-clock scheduler delays. It
     * increments heartbeat-missed and liveness-failure counters when the last
     * heartbeat timestamp is older than the configured timeout, then requests
     * autonomous reset recovery with {@link RecoveryReasonCode#HEARTBEAT_TIMEOUT}.</p>
     */
    void detectLivenessTimeout() {
        if (!livenessMonitoringActive) {
            return;
        }
        long lastHeartbeatNanos = counters().lastHeartbeatReceivedNanos().get();
        if (lastHeartbeatNanos <= 0) {
            return;
        }
        long elapsedNanos = nanoClock().nanoTime() - lastHeartbeatNanos;
        if (elapsedNanos >= heartbeatTimeoutNanos) {
            counters().heartbeatsMissed().increment();
            counters().livenessFailures().increment();
            counters().lastHeartbeatReceivedNanos().set(nanoClock().nanoTime());
            requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.HEARTBEAT_TIMEOUT,
                    "Coinbase L2 heartbeat timeout");
        }
    }

    /**
     * Returns whether liveness monitoring has started for the current session.
     *
     * @return true after subscription acknowledgement validation starts monitoring
     */
    boolean livenessMonitoringActive() {
        return livenessMonitoringActive;
    }

    /**
     * Returns the connector's per-instrument counters for package-local liveness tests.
     *
     * @return counter bundle cached by the base connector during initialization
     */
    InstrumentCounters instrumentCounters() {
        return counters();
    }

    private void recoveryUnsubscribe() {
        sendUnsubscribe();
    }

    private void recoveryClose() {
        NettyWebSocketTransport currentTransport = transport;
        if (currentTransport != null) {
            currentTransport.close();
        }
        stopLivenessMonitoring();
        markConnectionClosed();
    }

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

    private void recoveryResubscribe() {
        sendSubscribe();
    }

    private NettyWebSocketTransport requireTransport() {
        if (transport == null) {
            throw new IllegalStateException("Coinbase L2 transport has not been initialized");
        }
        return transport;
    }

    private static SslContext sslContextFor(URI endpoint) {
        if (!"wss".equalsIgnoreCase(endpoint.getScheme())) {
            return null;
        }
        try {
            return NettyWebSocketTransport.buildClientSslContext();
        } catch (SSLException ex) {
            throw new IllegalStateException("Unable to build Coinbase L2 TLS context", ex);
        }
    }

    private void markConnectionOpen() {
        if (!connectionGaugeActive) {
            connectionGaugeActive = true;
            gatewayCounters().activeConnections().increment();
        }
    }

    private void markConnectionClosed() {
        if (connectionGaugeActive) {
            connectionGaugeActive = false;
            gatewayCounters().activeConnections().decrement();
        }
    }

    private void stopLivenessMonitoring() {
        livenessMonitoringActive = false;
        ScheduledFuture<?> currentTask = livenessTask;
        if (currentTask != null) {
            currentTask.cancel(false);
            livenessTask = null;
        }
    }

    private void runLivenessCheckSafely() {
        try {
            detectLivenessTimeout();
        } catch (RuntimeException ex) {
            counters().livenessFailures().increment();
            throw ex;
        }
    }

    private static final class DeferredRecoveryActions implements CoinbaseL2RecoveryStrategy.Actions {
        private CoinbaseL2Connector delegate;

        @Override
        public void unsubscribe() {
            requireDelegate().recoveryUnsubscribe();
        }

        @Override
        public void close() {
            requireDelegate().recoveryClose();
        }

        @Override
        public void reconnect() throws IOException {
            requireDelegate().recoveryReconnect();
        }

        @Override
        public void resubscribe() {
            requireDelegate().recoveryResubscribe();
        }

        private CoinbaseL2Connector requireDelegate() {
            if (delegate == null) {
                throw new IllegalStateException("Coinbase recovery actions are not attached to a connector");
            }
            return delegate;
        }
    }
}
