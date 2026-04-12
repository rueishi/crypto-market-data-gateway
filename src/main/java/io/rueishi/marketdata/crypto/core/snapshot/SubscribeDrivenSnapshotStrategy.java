package io.rueishi.marketdata.crypto.core.snapshot;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;

/**
 * Base class for venues whose snapshot arrives automatically after subscription.
 *
 * <p>Connectors call {@link #triggerSnapshot(InstrumentConfig, SnapshotContext)}
 * after subscribe or resubscribe just as they would for any other
 * {@link SnapshotStrategy}. In subscribe-driven feeds, however, no explicit
 * request is required: the venue's parser recognizes the incoming snapshot
 * frame, publishes it as the appropriate snapshot message, opens the shared
 * {@link SnapshotGatekeeper}, and calls
 * {@link SnapshotContext#onSnapshotBoundaryAccepted()}.</p>
 *
 * <p>Concrete venue classes typically extend this class without adding any
 * methods. The class is stateless and does not retain the instrument or context
 * passed by the connector.</p>
 */
public abstract class SubscribeDrivenSnapshotStrategy implements SnapshotStrategy {

    /**
     * Reports that this strategy expects the snapshot from the subscribed feed.
     *
     * @return {@link Mode#SUBSCRIBE_DRIVEN}
     */
    @Override
    public final Mode mode() {
        return Mode.SUBSCRIBE_DRIVEN;
    }

    /**
     * Performs no explicit snapshot work for subscribe-driven feeds.
     *
     * <p>The connector still calls this method to keep lifecycle flow uniform
     * across all snapshot modes. The parser later accepts the snapshot boundary
     * when the feed-delivered snapshot frame arrives.</p>
     *
     * @param instrument configured instrument for the current subscription; intentionally unused
     * @param ctx snapshot context for the current session; intentionally unused
     */
    @Override
    public final void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
        // No explicit request is needed; the subscribed feed delivers the snapshot frame.
    }
}
