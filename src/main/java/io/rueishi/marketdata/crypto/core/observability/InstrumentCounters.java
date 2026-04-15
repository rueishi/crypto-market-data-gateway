package io.rueishi.marketdata.crypto.core.observability;

import java.util.Objects;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Per-instrument observability counter bundle for connector hot-path work.
 *
 * <p>{@code InstrumentCounters} is created by {@link GatewayCounters} when a
 * connector asks for counters for its configured instrument. In the intended
 * runtime flow, bootstrap creates one {@code GatewayCounters} facade, connector
 * initialization calls {@link GatewayCounters#forInstrument(int)}, and the
 * connector, parser, encoder, publisher, recovery strategy, and liveness checks
 * share the returned object for that connector's lifetime.</p>
 *
 * <p>This class does not allocate counters directly from Agrona. Instead,
 * {@code GatewayCounters} passes a {@link CounterFactory} that applies the
 * gateway label dimensions plus the instrument id, then returns a real Agrona
 * {@link AtomicCounter}. This keeps label and allocation policy in one place
 * while giving hot-path code direct stable counter references.</p>
 *
 * <p>All counters are allocated during construction. Accessor calls only return
 * existing references and therefore do not allocate new counters or mutate
 * labels on the hot path. The Phase 2 Coinbase live path uses the heartbeat,
 * unknown-type, product-mismatch, unknown-symbol, subscription-validation, and
 * liveness timestamp counters exposed here; HTTP metrics export and persistent
 * error recording remain outside this bundle.</p>
 */
public final class InstrumentCounters {
    private final AtomicCounter framesReceived;
    private final AtomicCounter bytesReceived;
    private final AtomicCounter messagesDecoded;
    private final AtomicCounter snapshotMessagesReceived;
    private final AtomicCounter updateMessagesReceived;
    private final AtomicCounter heartbeatsReceived;
    private final AtomicCounter parseFailures;
    private final AtomicCounter malformedRejections;
    private final AtomicCounter authenticationErrors;
    private final AtomicCounter unknownTypeDrops;
    private final AtomicCounter preSnapshotDrops;
    private final AtomicCounter unknownSymbolDrops;
    private final AtomicCounter productIdMismatches;
    private final AtomicCounter encodeSuccesses;
    private final AtomicCounter encodeFailures;
    private final AtomicCounter overflowRejections;
    private final AtomicCounter levelsEncoded;
    private final AtomicCounter encodedBytes;
    private final AtomicCounter encodeBufferReuseCount;
    private final AtomicCounter messagesPublished;
    private final AtomicCounter publishFailures;
    private final AtomicCounter backpressureEvents;
    private final AtomicCounter backpressureDrops;
    private final AtomicCounter recoveryAttempts;
    private final AtomicCounter recoveryCompletions;
    private final AtomicCounter recoveryFailures;
    private final AtomicCounter recoveryExecutions;
    private final AtomicCounter recoveryRequestsIgnoredInProgress;
    private final AtomicCounter bookResetPublished;
    private final AtomicCounter bookSnapshotPublished;
    private final AtomicCounter subscriptionValidationFailures;
    private final AtomicCounter sequenceGapsDetected;
    private final AtomicCounter outOfOrderMessages;
    private final AtomicCounter checksumFailures;
    private final AtomicCounter heartbeatsMissed;
    private final AtomicCounter livenessFailures;
    private final AtomicCounter handoffLatencyMinNanos;
    private final AtomicCounter handoffLatencyMaxNanos;
    private final AtomicCounter handoffLatencyLastNanos;
    private final AtomicCounter publisherLatencyMinNanos;
    private final AtomicCounter publisherLatencyMaxNanos;
    private final AtomicCounter publisherLatencyLastNanos;
    private final AtomicCounter lastHeartbeatReceivedNanos;
    private final AtomicCounter lastMessageReceivedNanos;

    /**
     * Allocates the full per-instrument counter bundle through the supplied factory.
     *
     * @param factory allocation callback supplied by {@link GatewayCounters}
     * @param instrumentId configured instrument id added to every counter label
     * @throws NullPointerException if {@code factory} is null
     */
    InstrumentCounters(CounterFactory factory, int instrumentId) {
        Objects.requireNonNull(factory, "factory");

        this.framesReceived = factory.newCounter(instrumentId, "frames_received");
        this.bytesReceived = factory.newCounter(instrumentId, "bytes_received");
        this.messagesDecoded = factory.newCounter(instrumentId, "messages_decoded");
        this.snapshotMessagesReceived = factory.newCounter(instrumentId, "snapshot_messages_received");
        this.updateMessagesReceived = factory.newCounter(instrumentId, "update_messages_received");
        this.heartbeatsReceived = factory.newCounter(instrumentId, "heartbeats_received");
        this.parseFailures = factory.newCounter(instrumentId, "parse_failures");
        this.malformedRejections = factory.newCounter(instrumentId, "malformed_rejections");
        this.authenticationErrors = factory.newCounter(instrumentId, "authentication_errors");
        this.unknownTypeDrops = factory.newCounter(instrumentId, "unknown_type_drops");
        this.preSnapshotDrops = factory.newCounter(instrumentId, "pre_snapshot_drops");
        this.unknownSymbolDrops = factory.newCounter(instrumentId, "unknown_symbol_drops");
        this.productIdMismatches = factory.newCounter(instrumentId, "product_id_mismatches");
        this.encodeSuccesses = factory.newCounter(instrumentId, "encode_successes");
        this.encodeFailures = factory.newCounter(instrumentId, "encode_failures");
        this.overflowRejections = factory.newCounter(instrumentId, "overflow_rejections");
        this.levelsEncoded = factory.newCounter(instrumentId, "levels_encoded");
        this.encodedBytes = factory.newCounter(instrumentId, "encoded_bytes");
        this.encodeBufferReuseCount = factory.newCounter(instrumentId, "encode_buffer_reuse_count");
        this.messagesPublished = factory.newCounter(instrumentId, "messages_published");
        this.publishFailures = factory.newCounter(instrumentId, "publish_failures");
        this.backpressureEvents = factory.newCounter(instrumentId, "backpressure_events");
        this.backpressureDrops = factory.newCounter(instrumentId, "backpressure_drops");
        this.recoveryAttempts = factory.newCounter(instrumentId, "recovery_attempts");
        this.recoveryCompletions = factory.newCounter(instrumentId, "recovery_completions");
        this.recoveryFailures = factory.newCounter(instrumentId, "recovery_failures");
        this.recoveryExecutions = factory.newCounter(instrumentId, "recovery_executions");
        this.recoveryRequestsIgnoredInProgress =
                factory.newCounter(instrumentId, "recovery_requests_ignored_in_progress");
        this.bookResetPublished = factory.newCounter(instrumentId, "book_reset_published");
        this.bookSnapshotPublished = factory.newCounter(instrumentId, "book_snapshot_published");
        this.subscriptionValidationFailures =
                factory.newCounter(instrumentId, "subscription_validation_failures");
        this.sequenceGapsDetected = factory.newCounter(instrumentId, "sequence_gaps_detected");
        this.outOfOrderMessages = factory.newCounter(instrumentId, "out_of_order_messages");
        this.checksumFailures = factory.newCounter(instrumentId, "checksum_failures");
        this.heartbeatsMissed = factory.newCounter(instrumentId, "heartbeats_missed");
        this.livenessFailures = factory.newCounter(instrumentId, "liveness_failures");
        this.handoffLatencyMinNanos = factory.newCounter(instrumentId, "handoff_latency_min_nanos");
        this.handoffLatencyMaxNanos = factory.newCounter(instrumentId, "handoff_latency_max_nanos");
        this.handoffLatencyLastNanos = factory.newCounter(instrumentId, "handoff_latency_last_nanos");
        this.publisherLatencyMinNanos = factory.newCounter(instrumentId, "publisher_latency_min_nanos");
        this.publisherLatencyMaxNanos = factory.newCounter(instrumentId, "publisher_latency_max_nanos");
        this.publisherLatencyLastNanos = factory.newCounter(instrumentId, "publisher_latency_last_nanos");
        this.lastHeartbeatReceivedNanos = factory.newCounter(instrumentId, "last_heartbeat_received_nanos");
        this.lastMessageReceivedNanos = factory.newCounter(instrumentId, "last_message_received_nanos");
    }

    /** @return inbound frame counter for this instrument */
    public AtomicCounter framesReceived() {
        return framesReceived;
    }

    /** @return inbound byte counter for this instrument */
    public AtomicCounter bytesReceived() {
        return bytesReceived;
    }

    /** @return decoded message counter for this instrument */
    public AtomicCounter messagesDecoded() {
        return messagesDecoded;
    }

    /** @return snapshot message counter for this instrument */
    public AtomicCounter snapshotMessagesReceived() {
        return snapshotMessagesReceived;
    }

    /** @return update message counter for this instrument */
    public AtomicCounter updateMessagesReceived() {
        return updateMessagesReceived;
    }

    /** @return heartbeat received counter for this instrument */
    public AtomicCounter heartbeatsReceived() {
        return heartbeatsReceived;
    }

    /** @return parse failure counter for this instrument */
    public AtomicCounter parseFailures() {
        return parseFailures;
    }

    /** @return malformed rejection counter for this instrument */
    public AtomicCounter malformedRejections() {
        return malformedRejections;
    }

    /** @return inbound venue authentication error counter for this instrument */
    public AtomicCounter authenticationErrors() {
        return authenticationErrors;
    }

    /** @return unknown type drop counter for this instrument */
    public AtomicCounter unknownTypeDrops() {
        return unknownTypeDrops;
    }

    /** @return pre-snapshot drop counter for this instrument */
    public AtomicCounter preSnapshotDrops() {
        return preSnapshotDrops;
    }

    /** @return unknown symbol drop counter for this instrument */
    public AtomicCounter unknownSymbolDrops() {
        return unknownSymbolDrops;
    }

    /** @return product id mismatch counter for this instrument */
    public AtomicCounter productIdMismatches() {
        return productIdMismatches;
    }

    /** @return encode success counter for this instrument */
    public AtomicCounter encodeSuccesses() {
        return encodeSuccesses;
    }

    /** @return encode failure counter for this instrument */
    public AtomicCounter encodeFailures() {
        return encodeFailures;
    }

    /** @return overflow rejection counter for this instrument */
    public AtomicCounter overflowRejections() {
        return overflowRejections;
    }

    /** @return encoded level-entry counter for this instrument */
    public AtomicCounter levelsEncoded() {
        return levelsEncoded;
    }

    /** @return encoded byte counter for this instrument */
    public AtomicCounter encodedBytes() {
        return encodedBytes;
    }

    /** @return encode buffer reuse counter for this instrument */
    public AtomicCounter encodeBufferReuseCount() {
        return encodeBufferReuseCount;
    }

    /** @return published message counter for this instrument */
    public AtomicCounter messagesPublished() {
        return messagesPublished;
    }

    /** @return publish failure counter for this instrument */
    public AtomicCounter publishFailures() {
        return publishFailures;
    }

    /** @return backpressure event counter for this instrument */
    public AtomicCounter backpressureEvents() {
        return backpressureEvents;
    }

    /** @return backpressure drop counter for this instrument */
    public AtomicCounter backpressureDrops() {
        return backpressureDrops;
    }

    /** @return recovery attempt counter for this instrument */
    public AtomicCounter recoveryAttempts() {
        return recoveryAttempts;
    }

    /** @return recovery completion counter for this instrument */
    public AtomicCounter recoveryCompletions() {
        return recoveryCompletions;
    }

    /** @return recovery failure counter for this instrument */
    public AtomicCounter recoveryFailures() {
        return recoveryFailures;
    }

    /** @return recovery execution counter for this instrument */
    public AtomicCounter recoveryExecutions() {
        return recoveryExecutions;
    }

    /** @return ignored duplicate recovery request counter for this instrument */
    public AtomicCounter recoveryRequestsIgnoredInProgress() {
        return recoveryRequestsIgnoredInProgress;
    }

    /** @return BOOK_RESET publication counter for this instrument */
    public AtomicCounter bookResetPublished() {
        return bookResetPublished;
    }

    /** @return BOOK_SNAPSHOT publication counter for this instrument */
    public AtomicCounter bookSnapshotPublished() {
        return bookSnapshotPublished;
    }

    /** @return subscription validation failure counter for this instrument */
    public AtomicCounter subscriptionValidationFailures() {
        return subscriptionValidationFailures;
    }

    /** @return sequence gap counter for this instrument */
    public AtomicCounter sequenceGapsDetected() {
        return sequenceGapsDetected;
    }

    /** @return out-of-order message counter for this instrument */
    public AtomicCounter outOfOrderMessages() {
        return outOfOrderMessages;
    }

    /** @return checksum failure counter for this instrument */
    public AtomicCounter checksumFailures() {
        return checksumFailures;
    }

    /** @return missed heartbeat counter for this instrument */
    public AtomicCounter heartbeatsMissed() {
        return heartbeatsMissed;
    }

    /** @return liveness failure counter for this instrument */
    public AtomicCounter livenessFailures() {
        return livenessFailures;
    }

    /** @return minimum handoff latency counter for this instrument */
    public AtomicCounter handoffLatencyMinNanos() {
        return handoffLatencyMinNanos;
    }

    /** @return maximum handoff latency counter for this instrument */
    public AtomicCounter handoffLatencyMaxNanos() {
        return handoffLatencyMaxNanos;
    }

    /** @return last handoff latency counter for this instrument */
    public AtomicCounter handoffLatencyLastNanos() {
        return handoffLatencyLastNanos;
    }

    /** @return minimum publisher latency counter for this instrument */
    public AtomicCounter publisherLatencyMinNanos() {
        return publisherLatencyMinNanos;
    }

    /** @return maximum publisher latency counter for this instrument */
    public AtomicCounter publisherLatencyMaxNanos() {
        return publisherLatencyMaxNanos;
    }

    /** @return last publisher latency counter for this instrument */
    public AtomicCounter publisherLatencyLastNanos() {
        return publisherLatencyLastNanos;
    }

    /** @return last heartbeat timestamp counter for this instrument */
    public AtomicCounter lastHeartbeatReceivedNanos() {
        return lastHeartbeatReceivedNanos;
    }

    /** @return last inbound message timestamp counter for this instrument */
    public AtomicCounter lastMessageReceivedNanos() {
        return lastMessageReceivedNanos;
    }

    /**
     * Allocation callback used by {@link GatewayCounters} to apply gateway and instrument labels.
     */
    @FunctionalInterface
    interface CounterFactory {

        /**
         * Allocates one counter for the supplied instrument id and logical counter name.
         *
         * @param instrumentId configured instrument id to include in labels
         * @param name logical counter name without gateway labels
         * @return the allocated Agrona counter
         */
        AtomicCounter newCounter(int instrumentId, String name);
    }
}
