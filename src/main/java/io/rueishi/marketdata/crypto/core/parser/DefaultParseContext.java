package io.rueishi.marketdata.crypto.core.parser;

import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.sequence.SequenceTracker;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotGatekeeper;
import java.util.Objects;
import org.agrona.concurrent.NanoClock;

/**
 * Default connector-owned implementation of {@link ParseContext}.
 *
 * <p>{@code DefaultParseContext} is allocated once during connector
 * initialization and passed to {@link FeedParser} on every inbound frame. Fixed
 * dependencies such as encoder, publisher, counters, and clock remain stable
 * for the connector lifetime. Recovery renews only the session-scoped
 * {@link SequenceTracker} and {@link SnapshotGatekeeper} by calling
 * {@link #resetSession()} on the same context object. For subscribe-driven
 * venues, the parser also uses this context to report that the feed-delivered
 * snapshot boundary has been accepted. Parser-detected recovery triggers are
 * routed through a connector-supplied callback so venue parser classes do not
 * retain connector or recovery state. Normal parser publication also flows
 * through this context so retry, recovery, and ingress-to-handoff latency
 * policy is implemented once outside venue parser classes.</p>
 */
public final class DefaultParseContext implements ParseContext {
    private static final int PUBLISH_ATTEMPTS = 3;
    private static final RecoveryRequestCallback NOOP_RECOVERY_CALLBACK = (type, reason, diagnosticText) -> {
    };
    private static final Runnable NOOP_SUBSCRIPTION_ACK_CALLBACK = () -> {
    };
    private static final Runnable NOOP_SNAPSHOT_BOUNDARY_CALLBACK = () -> {
    };

    private final SbeEncoder encoder;
    private final SbeEncoder tradeEncoder;
    private final Publisher publisher;
    private final InstrumentCounters counters;
    private final NanoClock nanoClock;
    private final RecoveryRequestCallback recoveryRequestCallback;
    private final Runnable onSubscriptionAckValidated;
    private final Runnable onSnapshotBoundaryAccepted;

    private SequenceTracker sequenceTracker;
    private SnapshotGatekeeper snapshotGatekeeper;
    private boolean subscriptionAckValidated;
    private boolean snapshotBoundaryAcceptedSignaled;

    /**
     * Creates a parse context with stable connector-lifetime dependencies and an initial session.
     *
     * @param encoder connector-owned encoder
     * @param publisher shared publisher used by encoder finalization
     * @param counters per-instrument counters for this connector
     * @param nanoClock connector-local nanosecond clock
     * @throws NullPointerException if any argument is null
     */
    public DefaultParseContext(
            SbeEncoder encoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock) {
        this(
                encoder,
                encoder,
                publisher,
                counters,
                nanoClock,
                NOOP_RECOVERY_CALLBACK,
                NOOP_SUBSCRIPTION_ACK_CALLBACK,
                NOOP_SNAPSHOT_BOUNDARY_CALLBACK);
    }

    /**
     * Creates a parse context with a connector-owned snapshot-boundary callback.
     *
     * <p>Connectors use this constructor when their parser can accept a
     * subscribe-driven snapshot directly from the feed but does not need a
     * subscription-acknowledgement callback. Tests and contexts that do not need
     * connector lifecycle signaling can use the four-argument constructor, which
     * installs no-op callbacks.</p>
     *
     * @param encoder connector-owned encoder
     * @param publisher shared publisher used by encoder finalization
     * @param counters per-instrument counters for this connector
     * @param nanoClock connector-local nanosecond clock
     * @param onSnapshotBoundaryAccepted callback invoked once after a parser accepts the session snapshot
     * @throws NullPointerException if any argument is null
     */
    public DefaultParseContext(
            SbeEncoder encoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock,
            Runnable onSnapshotBoundaryAccepted) {
        this(
                encoder,
                encoder,
                publisher,
                counters,
                nanoClock,
                NOOP_RECOVERY_CALLBACK,
                NOOP_SUBSCRIPTION_ACK_CALLBACK,
                onSnapshotBoundaryAccepted);
    }

    /**
     * Creates a parse context with connector-owned session boundary callbacks.
     *
     * <p>Connectors use this constructor when their parser can validate
     * subscription acknowledgements or accept subscribe-driven snapshots. The
     * callbacks are invoked from the parser's event-loop path and reset with
     * the session state.</p>
     *
     * @param encoder connector-owned encoder
     * @param publisher shared publisher used by encoder finalization
     * @param counters per-instrument counters for this connector
     * @param nanoClock connector-local nanosecond clock
     * @param onSubscriptionAckValidated callback invoked once the parser validates the session subscription ack
     * @param onSnapshotBoundaryAccepted callback invoked once after a parser accepts the session snapshot
     * @throws NullPointerException if any argument is null
     */
    public DefaultParseContext(
            SbeEncoder encoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock,
            Runnable onSubscriptionAckValidated,
            Runnable onSnapshotBoundaryAccepted) {
        this(
                encoder,
                encoder,
                publisher,
                counters,
                nanoClock,
                NOOP_RECOVERY_CALLBACK,
                onSubscriptionAckValidated,
                onSnapshotBoundaryAccepted);
    }

    /**
     * Creates a parse context with connector-owned recovery and session boundary callbacks.
     *
     * <p>Connectors use this constructor when venue parsers can autonomously
     * request recovery from integrity failures. The callback is invoked only on
     * rare failure branches and is expected to build a connector-scoped
     * recovery request with venue, instrument, timestamp, and duplicate
     * coalescing handled outside the parser.</p>
     *
     * @param encoder connector-owned encoder
     * @param publisher shared publisher used by encoder finalization
     * @param counters per-instrument counters for this connector
     * @param nanoClock connector-local nanosecond clock
     * @param recoveryRequestCallback callback invoked when parser logic requests recovery
     * @param onSubscriptionAckValidated callback invoked once the parser validates the session subscription ack
     * @param onSnapshotBoundaryAccepted callback invoked once after a parser accepts the session snapshot
     * @throws NullPointerException if any argument is null
     */
    public DefaultParseContext(
            SbeEncoder encoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock,
            RecoveryRequestCallback recoveryRequestCallback,
            Runnable onSubscriptionAckValidated,
            Runnable onSnapshotBoundaryAccepted) {
        this(
                encoder,
                encoder,
                publisher,
                counters,
                nanoClock,
                recoveryRequestCallback,
                onSubscriptionAckValidated,
                onSnapshotBoundaryAccepted);
    }

    /**
     * Creates a parse context with separate stable encoders for order and trade templates.
     *
     * <p>Coinbase L3 and similar schema v2 venues publish both
     * {@code ORDER_EVENT} and {@code TRADE_EVENT} messages. Those parsers use
     * {@code encoder} for order lifecycle messages and {@code tradeEncoder} for
     * trade messages while still sharing the same publisher, counters, clock,
     * and session state.</p>
     *
     * @param encoder connector-owned primary encoder
     * @param tradeEncoder connector-owned trade encoder
     * @param publisher shared publisher used by encoder finalization
     * @param counters per-instrument counters for this connector
     * @param nanoClock connector-local nanosecond clock
     * @param recoveryRequestCallback callback invoked when parser logic requests recovery
     * @param onSubscriptionAckValidated callback invoked once the parser validates the session subscription ack
     * @param onSnapshotBoundaryAccepted callback invoked once after a parser accepts the session snapshot
     * @throws NullPointerException if any argument is null
     */
    public DefaultParseContext(
            SbeEncoder encoder,
            SbeEncoder tradeEncoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock,
            RecoveryRequestCallback recoveryRequestCallback,
            Runnable onSubscriptionAckValidated,
            Runnable onSnapshotBoundaryAccepted) {
        this.encoder = Objects.requireNonNull(encoder, "encoder");
        this.tradeEncoder = Objects.requireNonNull(tradeEncoder, "tradeEncoder");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.recoveryRequestCallback = Objects.requireNonNull(recoveryRequestCallback, "recoveryRequestCallback");
        this.onSubscriptionAckValidated =
                Objects.requireNonNull(onSubscriptionAckValidated, "onSubscriptionAckValidated");
        this.onSnapshotBoundaryAccepted =
                Objects.requireNonNull(onSnapshotBoundaryAccepted, "onSnapshotBoundaryAccepted");
        resetSession();
    }

    /**
     * Renews session-scoped sequence and snapshot state while preserving this context identity.
     *
     * <p>This allocation happens only at initialization or recovery boundaries,
     * never on the per-frame hot path. After this call, future
     * {@link #sequenceTracker()} and {@link #snapshotGatekeeper()} calls return
     * fresh session objects.</p>
     */
    @Override
    public void resetSession() {
        this.sequenceTracker = new SequenceTracker();
        this.snapshotGatekeeper = new SnapshotGatekeeper();
        this.subscriptionAckValidated = false;
        this.snapshotBoundaryAcceptedSignaled = false;
    }

    /**
     * Returns the currently installed snapshot gatekeeper for sharing with a new snapshot context.
     *
     * @return current session gatekeeper
     */
    public SnapshotGatekeeper currentSnapshotGatekeeper() {
        return snapshotGatekeeper;
    }

    /**
     * Marks the current session as subscription-acknowledged.
     *
     * <p>Coinbase Phase 2 parsers call this from the WebSocket event-loop path
     * after the {@code subscriptions} acknowledgement confirms all required
     * channels and the configured product id. The method also seeds the
     * heartbeat liveness timestamp so connector-owned checks do not treat the
     * subscribe-to-first-heartbeat gap as a timeout.</p>
     */
    @Override
    public void onSubscriptionAckValidated() {
        subscriptionAckValidated = true;
        counters.lastHeartbeatReceivedNanos().set(nanoClock.nanoTime());
        onSubscriptionAckValidated.run();
    }

    /**
     * Returns the current session subscription acknowledgement state.
     *
     * @return true only after the parser validates the current session acknowledgement
     */
    @Override
    public boolean subscriptionAckValidated() {
        return subscriptionAckValidated;
    }

    /**
     * Routes subscribe-driven snapshot boundary completion to connector lifecycle code exactly once.
     *
     * <p>The Coinbase L2 parser calls this after the snapshot message is
     * successfully published and the current {@link SnapshotGatekeeper} is
     * opened. Duplicate calls in the same session are rejected to avoid hiding
     * duplicate snapshot-boundary bugs, and calls before the gate opens fail
     * fast because they would otherwise complete recovery while updates remain
     * blocked.</p>
     *
     * @throws IllegalStateException if this session has already accepted a snapshot boundary or the gate is closed
     */
    @Override
    public void onSnapshotBoundaryAccepted() {
        if (!snapshotGatekeeper.isReady()) {
            throw new IllegalStateException("Snapshot gate must be open before accepting boundary");
        }
        if (snapshotBoundaryAcceptedSignaled) {
            throw new IllegalStateException("Snapshot boundary was already accepted for this parse session");
        }
        snapshotBoundaryAcceptedSignaled = true;
        onSnapshotBoundaryAccepted.run();
    }

    /**
     * Routes a parser-detected recovery trigger to the connector-owned callback.
     *
     * <p>The context validates required metadata but does not add venue,
     * instrument, timestamp, or coalescing behavior. Those details are owned by
     * the connector that installed the callback.</p>
     *
     * @param type requested recovery action
     * @param reason informational recovery reason
     * @param diagnosticText optional diagnostic text, or {@code null}
     * @throws NullPointerException if {@code type} or {@code reason} is null
     */
    @Override
    public void requestRecovery(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText) {
        recoveryRequestCallback.requestRecovery(
                Objects.requireNonNull(type, "type"),
                Objects.requireNonNull(reason, "reason"),
                diagnosticText);
    }

    /**
     * Finalizes the encoder buffer and publishes it with the fixed Phase 3 retry budget.
     *
     * <p>The encoder patches {@code entryCount} once, then this method attempts
     * up to three publisher calls against the same finalized buffer region. A
     * transient {@code false} result spins briefly and retries. Exhausted
     * backpressure increments drop/event counters, requests reset recovery as a
     * stream-integrity failure, and returns {@code false}. Publisher exceptions
     * are counted as publish failures and rethrown so the existing parser error
     * policy remains explicit.</p>
     *
     * @param diagnosticText optional diagnostic text for a backpressure recovery request
     * @return true when the publisher accepts the message, false after three rejected attempts
     * @throws RuntimeException if the publisher throws
     * @throws IllegalStateException if no encoder message is in progress
     */
    @Override
    public boolean publishEncodedMessage(String diagnosticText) {
        int length = encoder.finishMessage();
        for (int attempt = 0; attempt < PUBLISH_ATTEMPTS; attempt++) {
            long handoffLatencyNanos = nanoClock.nanoTime() - encoder.lastIngressTimestamp();
            try {
                if (publisher.publish(encoder.buffer(), 0, length, counters, nanoClock)) {
                    recordHandoffLatency(handoffLatencyNanos);
                    return true;
                }
            } catch (RuntimeException ex) {
                counters.publishFailures().increment();
                throw ex;
            }
            if (attempt < PUBLISH_ATTEMPTS - 1) {
                Thread.onSpinWait();
            }
        }
        counters.backpressureEvents().increment();
        counters.backpressureDrops().increment();
        requestRecovery(
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                diagnosticText);
        return false;
    }

    private void recordHandoffLatency(long latencyNanos) {
        counters.handoffLatencyLastNanos().set(latencyNanos);
        long currentMin = counters.handoffLatencyMinNanos().get();
        counters.handoffLatencyMinNanos().set(currentMin == 0L ? latencyNanos : Math.min(currentMin, latencyNanos));
        counters.handoffLatencyMaxNanos().set(Math.max(counters.handoffLatencyMaxNanos().get(), latencyNanos));
    }

    @Override
    public SequenceTracker sequenceTracker() {
        return sequenceTracker;
    }

    @Override
    public SnapshotGatekeeper snapshotGatekeeper() {
        return snapshotGatekeeper;
    }

    @Override
    public SbeEncoder encoder() {
        return encoder;
    }

    /**
     * Returns the connector-owned trade encoder installed for this parse context.
     *
     * <p>Contexts that do not need a second template simply return the same
     * encoder instance as {@link #encoder()}.</p>
     *
     * @return stable trade encoder reference
     */
    @Override
    public SbeEncoder tradeEncoder() {
        return tradeEncoder;
    }

    @Override
    public Publisher publisher() {
        return publisher;
    }

    @Override
    public NanoClock nanoClock() {
        return nanoClock;
    }

    @Override
    public InstrumentCounters counters() {
        return counters;
    }

    /**
     * Connector-owned callback used by parser code to request recovery without storing connector state.
     */
    @FunctionalInterface
    public interface RecoveryRequestCallback {
        /**
         * Handles one parser-originated recovery request.
         *
         * @param type requested recovery action
         * @param reason informational recovery reason
         * @param diagnosticText optional diagnostic text, or {@code null}
         */
        void requestRecovery(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText);
    }
}
