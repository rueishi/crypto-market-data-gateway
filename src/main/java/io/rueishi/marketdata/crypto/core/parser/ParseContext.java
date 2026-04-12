package io.rueishi.marketdata.crypto.core.parser;

import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.sequence.SequenceTracker;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotGatekeeper;
import org.agrona.concurrent.NanoClock;

/**
 * Carries session-scoped state and stable encoding resources into {@link FeedParser}.
 *
 * <p>{@code ParseContext} is a dependency carrier, not a parser facade. Venue
 * parsers call methods on the returned dependencies directly, such as
 * {@link #encoder()}, {@link #publisher()}, {@link #sequenceTracker()}, and
 * {@link #snapshotGatekeeper()}. The interface deliberately exposes no
 * pass-through encoding methods so parser implementations do not hide state
 * inside the context abstraction.</p>
 *
 * <p>A connector creates one {@link DefaultParseContext} during initialization
 * and keeps that object for its full lifetime. Recovery calls
 * {@link #resetSession()} on the same object to renew the session-local
 * {@link SequenceTracker} and {@link SnapshotGatekeeper} while preserving the
 * fixed encoder, publisher, counters, and clock dependencies. Parsers also use
 * {@link #onSubscriptionAckValidated()} to signal that control-plane
 * subscription acknowledgement has passed validation, allowing connector-owned
 * liveness checks to start after the exchange confirms the subscription. For
 * subscribe-driven feeds, parsers use {@link #onSnapshotBoundaryAccepted()} to
 * route the feed-delivered snapshot boundary back into connector lifecycle
 * state. Parser-detected integrity failures use
 * {@link #requestRecovery(RecoveryRequestType, RecoveryReasonCode, String)} so
 * parsers can stay stateless while connector-owned recovery remains the single
 * channel reset path. Normal parser publication goes through
 * {@link #publishEncodedMessage(String)} so bounded backpressure retry and
 * ingress-to-handoff latency measurement stay out of venue parser code.</p>
 */
public interface ParseContext {

    /**
     * Returns the session-local gateway sequence tracker.
     *
     * @return tracker reset at connector session boundaries
     */
    SequenceTracker sequenceTracker();

    /**
     * Returns the session-local snapshot gatekeeper.
     *
     * @return gatekeeper that blocks incremental updates until a snapshot is accepted
     */
    SnapshotGatekeeper snapshotGatekeeper();

    /**
     * Returns the connector-owned encoder used by the parser.
     *
     * @return stable encoder for this connector lifetime
     */
    SbeEncoder encoder();

    /**
     * Returns the downstream publisher used by encoder finalization.
     *
     * @return stable publisher dependency for this connector lifetime
     */
    Publisher publisher();

    /**
     * Returns the connector-local clock used for ingress timestamps.
     *
     * @return stable nanosecond clock dependency for this connector lifetime
     */
    NanoClock nanoClock();

    /**
     * Returns the per-instrument counters associated with the connector currently invoking the parser.
     *
     * @return stable per-instrument counter bundle for this connector lifetime
     */
    InstrumentCounters counters();

    /**
     * Records that the current session's subscription acknowledgement has passed venue validation.
     *
     * <p>Venue parsers call this after a control-plane acknowledgement proves
     * that the expected product and channels are active. Connectors can then
     * start heartbeat/liveness timing from a confirmed subscription boundary
     * instead of from the earlier transport-connect boundary.</p>
     */
    void onSubscriptionAckValidated();

    /**
     * Returns whether the current session has validated a subscription acknowledgement.
     *
     * @return true after {@link #onSubscriptionAckValidated()} and before the next {@link #resetSession()}
     */
    boolean subscriptionAckValidated();

    /**
     * Records that the current session's feed-delivered snapshot boundary was accepted.
     *
     * <p>Subscribe-driven venue parsers call this immediately after publishing
     * a snapshot and opening {@link #snapshotGatekeeper()}. Connector lifecycle
     * code uses the signal like {@link SnapshotContext#onSnapshotBoundaryAccepted()}
     * to complete Phase B of recovery. The method is expected to be one-shot per
     * session and reset by {@link #resetSession()}.</p>
     */
    void onSnapshotBoundaryAccepted();

    /**
     * Requests connector-owned recovery from a parser-detected integrity failure.
     *
     * <p>Venue parsers call this only on rare control-plane or stream-integrity
     * failure branches, such as invalid subscription acknowledgements or
     * out-of-order snapshot/update transitions. The implementing connector
     * supplies venue, instrument, timestamp, and coalescing behavior; parser
     * code supplies only the requested action, reason, and optional diagnostic
     * text. The diagnostic may be {@code null} on hot paths to avoid allocation.</p>
     *
     * @param type requested recovery action, normally {@link RecoveryRequestType#RESET}
     * @param reason informational reason code for observability and tests
     * @param diagnosticText optional off-path diagnostic text, or {@code null}
     * @throws NullPointerException if {@code type} or {@code reason} is null
     */
    void requestRecovery(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText);

    /**
     * Finalizes and publishes the current encoded message through connector-owned handoff policy.
     *
     * <p>Venue parsers call this after writing all entries into
     * {@link #encoder()}. The context patches the encoded entry count, attempts
     * the publisher handoff up to the fixed Phase 3 retry budget, records
     * ingress-to-handoff latency before a successful handoff, and requests
     * reset recovery if downstream backpressure is exhausted.</p>
     *
     * @param diagnosticText optional diagnostic text used if backpressure triggers recovery
     * @return true when the publisher accepts the message, false when bounded retries are exhausted
     * @throws RuntimeException if the publisher throws; the context records a publish failure first
     * @throws IllegalStateException if no encoder message is in progress
     */
    boolean publishEncodedMessage(String diagnosticText);

    /**
     * Renews session-local parser state for recovery or resubscription.
     *
     * <p>The implementing context object remains the same, but
     * {@link #sequenceTracker()} and {@link #snapshotGatekeeper()} return fresh
     * session state after this call.</p>
     */
    void resetSession();
}
