package io.rueishi.marketdata.crypto.core.snapshot;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;

/**
 * Strategy interface for obtaining the first usable book snapshot for a connector session.
 *
 * <p>The connector calls a {@code SnapshotStrategy} after subscribing or
 * resubscribing an instrument. Subscribe-driven venues receive the snapshot
 * through the normal feed and therefore do not need an explicit request. REST
 * then delta venues use this strategy boundary to start their off-path snapshot
 * fetch and delta alignment process. The strategy cooperates with
 * {@link SnapshotContext} to publish the snapshot, open the current
 * {@link SnapshotGatekeeper}, and signal snapshot boundary completion back to
 * connector lifecycle code.</p>
 *
 * <p>Venue-specific implementations are created during connector wiring and
 * reused across connector sessions. They must not retain the per-attempt
 * {@link SnapshotContext}; a fresh context is supplied for each session or
 * recovery attempt.</p>
 */
public interface SnapshotStrategy {

    /**
     * Snapshot acquisition mode used by the connector to understand strategy behavior.
     */
    enum Mode {
        /** Snapshot arrives automatically from the subscribed feed. */
        SUBSCRIBE_DRIVEN,
        /** Snapshot must be fetched separately and aligned with buffered deltas. */
        REST_THEN_DELTA
    }

    /**
     * Returns the snapshot acquisition mode implemented by this strategy.
     *
     * @return snapshot acquisition mode for the venue/depth implementation
     */
    Mode mode();

    /**
     * Starts snapshot acquisition for the supplied instrument and connector session.
     *
     * <p>For {@link Mode#SUBSCRIBE_DRIVEN} implementations this is intentionally
     * a no-op because the parser will observe the snapshot frame in the normal
     * feed. For {@link Mode#REST_THEN_DELTA} implementations, this method starts
     * the venue-specific REST snapshot and delta-buffer alignment flow.</p>
     *
     * @param instrument configured instrument whose snapshot is required
     * @param ctx session-scoped snapshot context containing gatekeeper, encoder, publisher, counters, and callback
     */
    void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx);
}
