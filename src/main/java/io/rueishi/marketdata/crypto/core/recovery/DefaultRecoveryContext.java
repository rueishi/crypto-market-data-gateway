package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Default one-shot implementation of {@link RecoveryContext}.
 *
 * <p>A connector creates a fresh {@code DefaultRecoveryContext} for each
 * recovery attempt. The context wraps the connector's Phase A success callback,
 * failure callback, and per-instrument counters. It accepts exactly one terminal
 * recovery signal so a stale or buggy strategy cannot invoke both success and
 * failure through the same attempt wrapper.</p>
 */
public final class DefaultRecoveryContext implements RecoveryContext {
    private final Runnable onChannelRestored;
    private final Consumer<String> onFailed;
    private final InstrumentCounters counters;
    private boolean signaled;

    /**
     * Creates a recovery context for one recovery attempt.
     *
     * @param onChannelRestored callback invoked when channel work succeeds
     * @param onFailed callback invoked when channel work fails
     * @param counters per-instrument counters for recovery events
     * @throws NullPointerException if any argument is null
     */
    public DefaultRecoveryContext(
            Runnable onChannelRestored,
            Consumer<String> onFailed,
            InstrumentCounters counters) {
        this.onChannelRestored = Objects.requireNonNull(onChannelRestored, "onChannelRestored");
        this.onFailed = Objects.requireNonNull(onFailed, "onFailed");
        this.counters = Objects.requireNonNull(counters, "counters");
    }

    /**
     * Invokes the recovery success callback for this attempt.
     *
     * @throws IllegalStateException if this context has already signaled success or failure
     */
    @Override
    public void onChannelRestored() {
        requireNotSignaled();
        signaled = true;
        onChannelRestored.run();
    }

    /**
     * Invokes the recovery failure callback for this attempt.
     *
     * @param reason human-readable failure reason
     * @throws IllegalStateException if this context has already signaled success or failure
     */
    @Override
    public void onRecoveryFailed(String reason) {
        requireNotSignaled();
        signaled = true;
        onFailed.accept(reason);
    }

    @Override
    public InstrumentCounters counters() {
        return counters;
    }

    private void requireNotSignaled() {
        if (signaled) {
            throw new IllegalStateException("RecoveryContext already signaled for this attempt");
        }
    }
}
