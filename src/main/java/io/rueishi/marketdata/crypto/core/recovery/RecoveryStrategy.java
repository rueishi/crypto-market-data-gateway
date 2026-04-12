package io.rueishi.marketdata.crypto.core.recovery;

/**
 * Strategy interface for executing connector recovery channel work.
 *
 * <p>The connector delegates to a {@code RecoveryStrategy} after it has decided
 * a recovery attempt is necessary. Implementations handle venue-specific work
 * such as unsubscribe, disconnect, reconnect, and resubscribe. They must then
 * call exactly one terminal callback on the supplied {@link RecoveryContext}:
 * {@link RecoveryContext#onChannelRestored()} when channel work succeeded, or
 * {@link RecoveryContext#onRecoveryFailed(String)} when it failed.</p>
 *
 * <p>The strategy does not own snapshot boundary completion. On success it only
 * finishes Phase A of recovery; connector snapshot code and
 * {@code SnapshotContext} finish Phase B when a valid snapshot boundary is
 * accepted.</p>
 */
public interface RecoveryStrategy {

    /**
     * Executes one recovery attempt for the supplied request.
     *
     * @param request immutable recovery request metadata
     * @param ctx attempt-scoped callback and counter context
     */
    void execute(RecoveryRequest request, RecoveryContext ctx);
}
