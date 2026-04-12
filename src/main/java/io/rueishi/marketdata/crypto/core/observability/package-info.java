/**
 * Phase 1 observability support.
 *
 * <p>Classes in this package own gateway and per-instrument counters used by
 * bootstrap, connectors, parsers, encoders, publishers, recovery code, and
 * liveness checks. Clock-bearing runtime contracts use Agrona's
 * {@code NanoClock} and {@code EpochClock} directly; this package intentionally
 * does not introduce local clock wrapper interfaces.</p>
 */
package io.rueishi.marketdata.crypto.core.observability;
