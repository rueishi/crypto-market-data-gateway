package io.rueishi.marketdata.crypto.core.snapshot;

/**
 * Gates incremental book-update publication until a valid snapshot boundary is accepted.
 *
 * <p>{@code SnapshotGatekeeper} is the session-local state holder shared by
 * parser and snapshot context code. Parsers check {@link #isReady()} before
 * publishing {@code BOOK_UPDATE} messages. Snapshot handling calls
 * {@link #accept()} after the first valid snapshot boundary is published, which
 * opens the gate for subsequent incremental updates in the same session.</p>
 *
 * <p>The gate starts closed. {@link #reset()} returns it to the waiting state at
 * the start of recovery or resubscription so updates received before the next
 * clean snapshot boundary are dropped rather than buffered or published out of
 * order.</p>
 */
public final class SnapshotGatekeeper {
    private boolean ready;

    /**
     * Returns whether incremental updates may be published in the current session.
     *
     * @return true only after {@link #accept()} has been called since construction or the last reset
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Opens the gate after the first valid snapshot boundary is accepted.
     *
     * <p>This operation is idempotent. Repeated calls keep the gate open and do
     * not allocate or alter any additional state.</p>
     */
    public void accept() {
        ready = true;
    }

    /**
     * Closes the gate for a new connector session.
     *
     * <p>After reset, parsers must treat inbound incremental updates as
     * pre-snapshot drops until snapshot handling calls {@link #accept()} again.</p>
     */
    public void reset() {
        ready = false;
    }
}
