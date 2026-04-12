package io.rueishi.marketdata.crypto.core.publisher;

import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;

/**
 * Shared helper for publisher-stage latency counters.
 *
 * <p>{@code PublisherLatencyStats} is used by publisher implementations after
 * they measure the time spent inside their own {@link Publisher#publish}
 * method. It updates only publisher-stage counters; connector handoff latency
 * is owned by connector code and must remain separate.</p>
 */
public final class PublisherLatencyStats {
    private PublisherLatencyStats() {
    }

    /**
     * Records one publisher-stage latency observation.
     *
     * <p>The minimum counter starts at {@code 0}, which means "no observations"
     * until the first value is recorded. A real observed latency may also be
     * zero when the provided clock is coarse, so after zero is recorded it can
     * remain the minimum.</p>
     *
     * @param counters per-instrument counter bundle to update
     * @param latencyNanos measured publisher-stage latency in nanoseconds
     * @throws NullPointerException if {@code counters} is null
     */
    public static void record(InstrumentCounters counters, long latencyNanos) {
        counters.publisherLatencyLastNanos().set(latencyNanos);
        long currentMin = counters.publisherLatencyMinNanos().get();
        long nextMin = currentMin == 0L ? latencyNanos : Math.min(currentMin, latencyNanos);
        counters.publisherLatencyMinNanos().set(nextMin);
        counters.publisherLatencyMaxNanos().set(
                Math.max(counters.publisherLatencyMaxNanos().get(), latencyNanos));
    }
}
