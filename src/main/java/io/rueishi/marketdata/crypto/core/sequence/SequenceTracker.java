package io.rueishi.marketdata.crypto.core.sequence;

/**
 * Tracks gateway-managed outbound sequence numbers for one connector session.
 *
 * <p>{@code SequenceTracker} is owned by the parser/context layer for a single
 * connector session. Venue parsers call {@link #next()} when they are about to
 * encode a publishable message and pass the returned value to the encoder as
 * {@code gatewayMessageSeq}. Venues without exchange-native sequence numbers,
 * such as Coinbase L2 in the spec, also use the same value for {@code seq1} and
 * {@code seq2}. Venues with native sequence ranges still use this tracker for
 * the gateway-managed sequence while parser code supplies exchange-native
 * {@code seq1}/{@code seq2} separately.</p>
 *
 * <p>The tracker starts at {@code 0}. The first call to {@link #next()} after
 * construction or {@link #reset()} returns {@code 1}. {@code reset()} is called
 * at the start of a recovery/session reset so every new session begins with a
 * deterministic sequence boundary.</p>
 */
public final class SequenceTracker {
    private long seq;

    /**
     * Increments the session-local sequence and returns the new value.
     *
     * <p>This method performs no allocation and has no side effect beyond
     * updating the internal counter. Parser code calls it exactly when a message
     * is accepted for encoding.</p>
     *
     * @return the next gateway-managed sequence number, starting at {@code 1}
     */
    public long next() {
        return ++seq;
    }

    /**
     * Returns the current sequence without incrementing it.
     *
     * <p>Tests and future diagnostics can use this to inspect the tracker state
     * without consuming a sequence number.</p>
     *
     * @return the current gateway-managed sequence number, or {@code 0} before the first message
     */
    public long current() {
        return seq;
    }

    /**
     * Resets the tracker to the beginning of a new connector session.
     *
     * <p>After this call, {@link #current()} returns {@code 0} and the next
     * {@link #next()} call returns {@code 1}. Recovery/context code calls this
     * when session state is renewed.</p>
     */
    public void reset() {
        seq = 0;
    }
}
