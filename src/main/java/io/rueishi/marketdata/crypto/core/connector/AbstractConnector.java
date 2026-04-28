package io.rueishi.marketdata.crypto.core.connector;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.recovery.DefaultRecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.snapshot.DefaultSnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import io.rueishi.marketdata.crypto.core.transport.WebSocketFrameHandler;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.NanoClock;

/**
 * Base implementation for one venue/instrument connector.
 *
 * <p>Venue connector classes extend this base and provide only fixed venue
 * declarations such as {@link #venueEnum()}, {@link #maxEntryCount()}, and
 * {@link #headroomBytes()}. The base class owns lifecycle boilerplate:
 * initialization from {@link ConnectorContext}, parser frame delegation,
 * connector-owned encoder and context construction, recovery coalescing,
 * two-phase recovery callbacks, and shutdown flags.</p>
 *
 * <p>The connector receives {@link ConnectorContext} once in
 * {@link #init(ConnectorContext)}, unpacks the needed resources, and never
 * stores the context wrapper. {@link DefaultParseContext} is allocated exactly
 * once and reset in place during recovery. {@link DefaultSnapshotContext} and
 * {@link DefaultRecoveryContext} are rebuilt per recovery phase so stale
 * callbacks cannot complete a later attempt.</p>
 *
 * <p>For subscribe-driven feeds, the parse context is also wired to the same
 * snapshot-accepted lifecycle callback used by {@link DefaultSnapshotContext},
 * because the venue parser sees the feed-delivered snapshot boundary before the
 * snapshot strategy can do any explicit work. Parser-detected integrity
 * failures are likewise routed through the parse context and converted here
 * into connector-scoped {@link RecoveryRequest} objects.</p>
 *
 * <p>The base class also owns the shutdown guard used by
 * {@link io.rueishi.marketdata.crypto.core.bootstrap.GatewayRuntime}: once
 * shutdown starts, new market-data frames and recovery requests are ignored so
 * reconnect work cannot race with unsubscribe/reset/close ordering.</p>
 *
 * <p>Phase 2 venue connectors use {@link #onInitialized(ConnectorContext)},
 * {@link #onConnect()}, {@link #onShutdown()},
 * {@link #onSubscriptionAckValidated()}, and
 * {@link #onTransportDisconnected(Throwable)} to attach transport and liveness
 * behavior while preserving the same parser, snapshot, and recovery state
 * machine.</p>
 */
public abstract class AbstractConnector implements Connector, WebSocketFrameHandler {
    private final InstrumentConfig instrument;
    private final FeedParser parser;
    private final SnapshotStrategy snapshot;
    private final RecoveryStrategy recovery;
    private final SubscriptionBuilder subscription;

    private Publisher publisher;
    private GatewayCounters gatewayCounters;
    private InstrumentCounters counters;
    private EventLoopGroup eventLoopGroup;
    private NanoClock clock;
    private SbeEncoder encoder;
    private DefaultParseContext parseCtx;
    private DefaultSnapshotContext snapshotCtx;
    private DefaultRecoveryContext recoveryCtx;
    private ExecutorService recoveryExecutor;
    private long shutdownDeadlineMs;
    private boolean initialized;
    private volatile boolean recoveryInProgress;
    private volatile boolean shutdownRequested;

    /**
     * Creates a connector with venue-specific collaborators supplied by a concrete factory.
     *
     * @param instrument configured instrument served by this connector
     * @param parser stateless venue parser used for inbound frames
     * @param snapshot snapshot strategy for this venue/depth
     * @param recovery recovery strategy for this venue/depth
     * @param subscription subscription payload builder for this venue/depth
     * @throws NullPointerException if any argument is null
     */
    protected AbstractConnector(
            InstrumentConfig instrument,
            FeedParser parser,
            SnapshotStrategy snapshot,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription) {
        this.instrument = Objects.requireNonNull(instrument, "instrument");
        this.parser = Objects.requireNonNull(parser, "parser");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.subscription = Objects.requireNonNull(subscription, "subscription");
    }

    /**
     * Returns the fixed venue/depth identity for this connector subclass.
     *
     * @return venue/depth identity used for encoding and request filtering
     */
    protected abstract VenueEnum venueEnum();

    /**
     * Returns the maximum number of entries the connector encoder can emit in one message.
     *
     * @return maximum repeating-group entry count for the encoder
     */
    protected abstract int maxEntryCount();

    /**
     * Returns extra encoder buffer capacity reserved beyond the maximum encoded payload.
     *
     * @return additional encoder buffer bytes
     */
    protected abstract int headroomBytes();

    /**
     * Creates the secondary trade encoder injected into the connector's {@link DefaultParseContext}.
     *
     * <p>The base implementation returns the same {@link SbeEncoder} instance as the primary
     * encoder, which is correct for venues that publish only one template. Venues that publish
     * a second template (such as {@code TRADE_EVENT} for Coinbase L3) override this hook to
     * allocate a dedicated encoder initialized with the trade template byte. The hook is called
     * exactly once per connector lifetime, after the primary encoder is allocated, during
     * {@link #init(ConnectorContext)}.</p>
     *
     * @return encoder to use as the trade encoder in {@link DefaultParseContext}; defaults to the primary encoder
     */
    protected SbeEncoder createTradeEncoder() {
        return encoder;
    }

    /**
     * Initializes connector-owned hot-path dependencies and session contexts.
     *
     * <p>The method unpacks the supplied {@link ConnectorContext} into stable
     * connector-owned fields, allocates a single {@link SbeEncoder}, allocates a
     * single {@link DefaultParseContext}, and creates the initial snapshot and
     * recovery contexts. The wrapper context is not stored after the call
     * returns.</p>
     *
     * @param ctx bootstrap dependency carrier for this connector
     * @throws IllegalStateException if called more than once
     * @throws NullPointerException if {@code ctx} or one of its required values is null
     */
    @Override
    public final void init(ConnectorContext ctx) {
        if (initialized) {
            throw new IllegalStateException("Connector already initialized");
        }
        Objects.requireNonNull(ctx, "ctx");
        this.publisher = Objects.requireNonNull(ctx.publisher(), "ctx.publisher");
        this.gatewayCounters = Objects.requireNonNull(ctx.counters(), "ctx.counters");
        this.counters = gatewayCounters.forInstrument(instrument.instrumentId);
        this.eventLoopGroup = Objects.requireNonNull(ctx.eventLoopGroup(), "ctx.eventLoopGroup");
        this.clock = Objects.requireNonNull(ctx.nanoClock(), "ctx.nanoClock");
        this.shutdownDeadlineMs = Math.max(1L, ctx.transportConfig().shutdownDeadlineMs);
        this.recoveryExecutor = Executors.newSingleThreadExecutor(recoveryThreadFactory());
        this.encoder = new SbeEncoder(
                instrument.instrumentId,
                venueEnum().byteValue(),
                venueEnum().bookDepth().byteValue(),
                venueEnum().templateId().byteValue(),
                maxEntryCount(),
                headroomBytes());
        buildInitialSessionContexts();
        onInitialized(ctx);
        initialized = true;
    }

    /**
     * Starts the Phase 1 connector lifecycle.
     *
     * <p>Because live WebSocket transport is deferred, this method performs the
     * control-flow actions that can be tested now: it builds a subscribe payload
     * via {@link SubscriptionBuilder}, passes it to {@link #onSubscribe(byte[])},
     * and triggers the configured snapshot strategy.</p>
     *
     * @throws IllegalStateException if the connector has not been initialized
     */
    @Override
    public final void connect() {
        requireInitialized();
        if (shutdownRequested) {
            throw new IllegalStateException("Connector is shutting down");
        }
        onConnect();
        sendSubscribe();
        snapshot.triggerSnapshot(instrument, snapshotCtx);
    }

    /**
     * Delegates a text WebSocket frame to the connector's parser.
     *
     * @param frame inbound text payload owned by the transport caller
     * @throws IllegalStateException if the connector has not been initialized
     */
    @Override
    public final void onTextFrame(ByteBuf frame) {
        requireInitialized();
        if (shutdownRequested) {
            return;
        }
        parser.onTextFrame(frame, parseCtx);
    }

    /**
     * Delegates a binary WebSocket frame to the connector's parser.
     *
     * @param frame inbound binary payload owned by the transport caller
     * @throws IllegalStateException if the connector has not been initialized
     */
    @Override
    public final void onBinaryFrame(ByteBuf frame) {
        requireInitialized();
        if (shutdownRequested) {
            return;
        }
        parser.onBinaryFrame(frame, parseCtx);
    }

    /**
     * Converts an unexpected disconnect into a recovery request for this connector.
     *
     * @param cause disconnect cause reported by the future transport layer
     * @throws IllegalStateException if the connector has not been initialized
     */
    @Override
    public final void onDisconnected(Throwable cause) {
        requireInitialized();
        onTransportDisconnected(cause);
        if (shutdownRequested || recoveryInProgress) {
            return;
        }
        requestRecovery(new RecoveryRequest(
                venueEnum(),
                instrument.instrumentId,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                clock.nanoTime(),
                cause == null ? null : cause.getMessage()));
    }

    /**
     * Publishes reset, delegates channel work, and manages two-phase recovery state.
     *
     * <p>Duplicate requests while {@code recoveryInProgress} is true are counted
     * and ignored. Phase A completion through {@link RecoveryContext#onChannelRestored()}
     * resets the session and starts snapshot acquisition while keeping the flag
     * true. Phase B completion through
     * {@link SnapshotContext#onSnapshotBoundaryAccepted()} clears the flag and
     * increments recovery completions.</p>
     *
     * @param request recovery request metadata
     * @throws IllegalStateException if the connector has not been initialized
     * @throws NullPointerException if {@code request} is null
     */
    @Override
    public final void recover(RecoveryRequest request) {
        requireInitialized();
        Objects.requireNonNull(request, "request");
        if (shutdownRequested) {
            return;
        }
        if (request.venue != venueEnum() || request.instrumentId != instrument.instrumentId) {
            return;
        }
        if (recoveryInProgress) {
            counters.recoveryRequestsIgnoredInProgress().increment();
            return;
        }

        recoveryInProgress = true;
        publisher.publishReset(
                instrument.instrumentId,
                venueEnum().byteValue(),
                venueEnum().bookDepth().byteValue(),
                venueEnum().templateId().byteValue(),
                clock);
        counters.recoveryAttempts().increment();
        try {
            recovery.execute(request, recoveryCtx);
        } catch (RuntimeException ex) {
            onRecoveryStrategyThrew(ex);
        }
    }

    /**
     * Schedules a recovery request on this connector's dedicated recovery path.
     *
     * <p>Downstream routing can arrive from a control-plane, publisher, parser,
     * or transport callback thread, while recovery mutates connector-owned
     * session, snapshot, and recovery state and may block during reconnect or
     * handshake work. Scheduling through the connector-owned single-threaded
     * recovery executor preserves serialized ownership without blocking the
     * Netty event-loop callback path. {@link #recover(RecoveryRequest)} remains
     * the synchronous final guard for tests, venue/instrument mismatches, and
     * duplicate in-progress coalescing.</p>
     *
     * @param request recovery request metadata
     * @throws IllegalStateException if the connector has not been initialized
     * @throws NullPointerException if {@code request} is null
     */
    @Override
    public final void requestRecovery(RecoveryRequest request) {
        requireInitialized();
        Objects.requireNonNull(request, "request");
        if (shutdownRequested) {
            return;
        }
        try {
            recoveryExecutor.execute(() -> recover(request));
        } catch (RejectedExecutionException ex) {
            if (!shutdownRequested) {
                throw ex;
            }
        }
    }

    /**
     * Performs graceful shutdown control actions.
     *
     * <p>The method is idempotent. It marks shutdown requested, emits a
     * best-effort unsubscribe payload through {@link #onUnsubscribe(byte[])},
     * lets the concrete connector close live resources, and publishes a reset
     * if initialization completed.</p>
     */
    @Override
    public final void shutdown() {
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;
        if (!initialized) {
            return;
        }
        sendUnsubscribe();
        onShutdown();
        stopRecoveryExecutor();
        publisher.publishReset(
                instrument.instrumentId,
                venueEnum().byteValue(),
                venueEnum().bookDepth().byteValue(),
                venueEnum().templateId().byteValue(),
                clock);
    }

    @Override
    public final VenueEnum venue() {
        return venueEnum();
    }

    @Override
    public final int instrumentId() {
        return instrument.instrumentId;
    }

    /**
     * Returns whether shutdown has been requested.
     *
     * @return true after {@link #shutdown()} is called
     */
    protected final boolean shutdownRequested() {
        return shutdownRequested;
    }

    /**
     * Hook called when the connector builds a subscribe payload.
     *
     * <p>The Phase 1 base implementation intentionally does nothing. Tests and
     * later transport integration can override this hook to observe or send the
     * payload without changing recovery state code.</p>
     *
     * @param payload venue subscribe payload bytes
     */
    protected void onSubscribe(byte[] payload) {
    }

    /**
     * Hook called when the connector builds an unsubscribe payload.
     *
     * @param payload venue unsubscribe payload bytes
     */
    protected void onUnsubscribe(byte[] payload) {
    }

    /**
     * Hook called after the base connector has unpacked {@link ConnectorContext} and built contexts.
     *
     * <p>Transport-backed venue connectors override this to create connector-
     * owned resources that require the event loop group, transport config,
     * clocks, or other bootstrap dependencies. The default implementation keeps
     * earlier test connectors and non-transport placeholders inert.</p>
     *
     * @param ctx bootstrap dependency carrier for this connector
     */
    protected void onInitialized(ConnectorContext ctx) {
    }

    /**
     * Hook called when the parser validates the current session subscription acknowledgement.
     *
     * <p>Venue connectors override this to start liveness monitoring after the
     * exchange confirms the expected channels and product id. The default
     * implementation keeps non-live test connectors inert.</p>
     */
    protected void onSubscriptionAckValidated() {
    }

    /**
     * Hook called at the start of {@link #connect()} before subscription is sent.
     *
     * <p>Venue connectors override this to establish transport connections. Any
     * runtime exception propagates to the caller and prevents subscribe and
     * snapshot trigger work from running against an unconnected channel.</p>
     */
    protected void onConnect() {
    }

    /**
     * Hook called during shutdown after the unsubscribe payload is emitted.
     *
     * <p>Transport-backed venue connectors override this to close network
     * resources. The hook is skipped when shutdown is called before
     * initialization, matching the rest of the base lifecycle behavior.</p>
     */
    protected void onShutdown() {
    }

    /**
     * Hook called when the transport reports channel loss or close.
     *
     * <p>Venue connectors override this to update connection gauges and stop
     * liveness tasks. The base connector invokes it before deciding whether the
     * disconnect should be converted into a recovery request; shutdown-initiated
     * disconnects therefore still update observability but do not start
     * recovery.</p>
     *
     * @param cause disconnect cause reported by the transport, or null for a normal close
     */
    protected void onTransportDisconnected(Throwable cause) {
    }

    /**
     * Returns the connector-owned parse context for test subclasses and future venue helpers.
     *
     * @return parse context allocated during init
     */
    public final DefaultParseContext parseContext() {
        return parseCtx;
    }

    /**
     * Returns the current snapshot context for test subclasses and future venue helpers.
     *
     * @return active snapshot context
     */
    public final DefaultSnapshotContext snapshotContext() {
        return snapshotCtx;
    }

    /**
     * Returns the current recovery context for test subclasses and future venue helpers.
     *
     * @return active recovery context
     */
    public final DefaultRecoveryContext recoveryContext() {
        return recoveryCtx;
    }

    /**
     * Returns the current recovery-in-progress state for tests and subclasses that need visibility.
     *
     * @return true while Phase A or Phase B recovery is active
     */
    public final boolean recoveryInProgress() {
        return recoveryInProgress;
    }

    /**
     * Returns the per-instrument counter bundle unpacked during initialization.
     *
     * @return connector counters
     */
    public final InstrumentCounters counters() {
        return counters;
    }

    /**
     * Returns the gateway-scoped counters unpacked during initialization.
     *
     * @return gateway counters shared by connectors in this process
     */
    protected final GatewayCounters gatewayCounters() {
        return gatewayCounters;
    }

    /**
     * Returns the connector-local nanosecond clock unpacked during initialization.
     *
     * @return connector clock used by parser, encoder, and liveness checks
     */
    protected final NanoClock nanoClock() {
        return clock;
    }

    /**
     * Returns the connector-owned encoder allocated during initialization.
     *
     * @return connector encoder
     */
    protected final SbeEncoder encoder() {
        return encoder;
    }

    /**
     * Builds and submits a connector-scoped recovery request from parser or liveness code.
     *
     * <p>Venue parsers supply only action, reason, and optional diagnostic text
     * through {@link DefaultParseContext}; liveness checks call this helper
     * from connector-owned monitoring tasks. The base connector fills in venue,
     * instrument id, and request timestamp before delegating to
     * {@link #requestRecovery(RecoveryRequest)}, whose scheduled
     * {@link #recover(RecoveryRequest)} call still uses duplicate coalescing as
     * the final guard.</p>
     *
     * @param type requested recovery action
     * @param reason informational recovery reason
     * @param diagnosticText optional diagnostic text, or {@code null}
     * @throws NullPointerException if {@code type} or {@code reason} is null
     */
    protected final void requestRecovery(
            RecoveryRequestType type,
            RecoveryReasonCode reason,
            String diagnosticText) {
        requestRecovery(new RecoveryRequest(
                venueEnum(),
                instrument.instrumentId,
                Objects.requireNonNull(type, "type"),
                Objects.requireNonNull(reason, "reason"),
                clock.nanoTime(),
                diagnosticText));
    }

    /**
     * Builds and emits a subscribe payload through the connector's venue hook.
     *
     * <p>Initial connect and venue-owned recovery actions use this helper so
     * authenticated payload construction stays in the shared base while the
     * concrete connector decides how to send the bytes.</p>
     */
    protected final void sendSubscribe() {
        onSubscribe(subscription.buildSubscribe(instrument));
    }

    /**
     * Builds and emits an unsubscribe payload through the connector's venue hook.
     *
     * <p>Shutdown and best-effort recovery unsubscribe use this helper. If the
     * venue transport is already closed, the concrete hook may skip the send or
     * throw depending on whether the caller treats the operation as best-effort.</p>
     */
    protected final void sendUnsubscribe() {
        onUnsubscribe(subscription.buildUnsubscribe(instrument));
    }

    private void onChannelRestored() {
        parseCtx.resetSession();
        rebuildSnapshotContext();
        rebuildRecoveryContext();
        snapshot.triggerSnapshot(instrument, snapshotCtx);
    }

    /**
     * Handles accepted snapshot boundaries from snapshot or parser contexts.
     *
     * <p>Initial connection snapshots also pass through this callback for
     * subscribe-driven feeds, so recovery completion counters are updated only
     * while a recovery attempt is active. During recovery this is Phase B
     * completion: it clears the coalescing flag and records one completed
     * recovery.</p>
     */
    private void onSnapshotAccepted() {
        if (recoveryInProgress) {
            recoveryInProgress = false;
            counters.recoveryCompletions().increment();
        }
    }

    private void onRecoveryFailed(String reason) {
        recoveryInProgress = false;
    }

    private void onRecoveryStrategyThrew(RuntimeException ex) {
        recoveryInProgress = false;
        counters.recoveryFailures().increment();
    }

    private void buildInitialSessionContexts() {
        this.parseCtx = new DefaultParseContext(
                encoder,
                createTradeEncoder(),
                publisher,
                counters,
                clock,
                this::requestRecovery,
                this::onSubscriptionAckValidated,
                this::onSnapshotAccepted);
        rebuildSnapshotContext();
        rebuildRecoveryContext();
    }

    private void rebuildSnapshotContext() {
        this.snapshotCtx = new DefaultSnapshotContext(
                parseCtx.currentSnapshotGatekeeper(),
                encoder,
                publisher,
                counters,
                clock,
                this::onSnapshotAccepted);
    }

    private void rebuildRecoveryContext() {
        this.recoveryCtx = new DefaultRecoveryContext(
                this::onChannelRestored,
                this::onRecoveryFailed,
                counters);
    }

    private void requireInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Connector must be initialized before use");
        }
    }

    private ThreadFactory recoveryThreadFactory() {
        return runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "recovery-" + venueEnum().name().toLowerCase() + "-" + instrument.instrumentId);
            thread.setDaemon(false);
            return thread;
        };
    }

    private void stopRecoveryExecutor() {
        ExecutorService currentRecoveryExecutor = recoveryExecutor;
        if (currentRecoveryExecutor == null) {
            return;
        }
        currentRecoveryExecutor.shutdown();
        if (!awaitRecoveryExecutor(currentRecoveryExecutor, shutdownDeadlineMs)) {
            currentRecoveryExecutor.shutdownNow();
            awaitRecoveryExecutor(currentRecoveryExecutor, Math.min(250L, shutdownDeadlineMs));
        }
        recoveryExecutor = null;
    }

    private static boolean awaitRecoveryExecutor(ExecutorService executor, long timeoutMs) {
        try {
            return executor.awaitTermination(Math.max(1L, timeoutMs), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
