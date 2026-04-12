package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;

/**
 * Carries recovery-strategy completion callbacks and counters.
 *
 * <p>Recovery strategies receive this context for one recovery attempt. They
 * signal channel-work success through {@link #onChannelRestored()} or failure
 * through {@link #onRecoveryFailed(String)}. Fresh snapshot arrival is not
 * signaled here; snapshot completion is reported later through
 * {@code SnapshotContext.onSnapshotBoundaryAccepted()}.</p>
 */
public interface RecoveryContext {

    /**
     * Signals that recovery channel work completed and snapshot acquisition can begin.
     *
     * <p>Connector lifecycle code uses this callback to reset parser session
     * state, rebuild snapshot/recovery contexts, resubscribe, and trigger the
     * snapshot strategy. The recovery-in-progress flag remains set until the
     * snapshot context reports boundary acceptance.</p>
     */
    void onChannelRestored();

    /**
     * Signals that recovery channel work failed.
     *
     * @param reason human-readable failure reason; callers should avoid including secrets
     */
    void onRecoveryFailed(String reason);

    /** @return per-instrument counters for this recovery attempt */
    InstrumentCounters counters();
}
