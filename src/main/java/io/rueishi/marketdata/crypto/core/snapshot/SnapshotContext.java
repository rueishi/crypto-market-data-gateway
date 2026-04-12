package io.rueishi.marketdata.crypto.core.snapshot;

import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import org.agrona.concurrent.NanoClock;

/**
 * Carries snapshot-strategy dependencies for accepting and publishing a snapshot boundary.
 *
 * <p>{@code SnapshotContext} is created by connector lifecycle code and passed
 * to snapshot strategies. Its {@link #snapshotGatekeeper()} must be the same
 * object currently exposed by the parse context so calling
 * {@link SnapshotGatekeeper#accept()} opens the parser's update gate
 * immediately. After publishing {@code BOOK_SNAPSHOT} and opening the gate, the
 * strategy calls {@link #onSnapshotBoundaryAccepted()} to complete the recovery
 * phase.</p>
 */
public interface SnapshotContext {

    /** @return gatekeeper shared with the current parse context */
    SnapshotGatekeeper snapshotGatekeeper();

    /** @return connector-owned encoder used to publish the snapshot */
    SbeEncoder encoder();

    /** @return publisher used by encoder finalization */
    Publisher publisher();

    /** @return per-instrument counters for this connector */
    InstrumentCounters counters();

    /** @return connector-local nanosecond clock */
    NanoClock nanoClock();

    /**
     * Signals that the snapshot boundary was published and accepted.
     *
     * <p>Connector lifecycle code uses this callback to clear recovery-in-progress
     * state and increment recovery completion counters.</p>
     */
    void onSnapshotBoundaryAccepted();
}
