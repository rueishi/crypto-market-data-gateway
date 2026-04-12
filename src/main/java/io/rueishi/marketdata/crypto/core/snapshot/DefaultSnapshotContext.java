package io.rueishi.marketdata.crypto.core.snapshot;

import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.util.Objects;
import org.agrona.concurrent.NanoClock;

/**
 * Default one-shot implementation of {@link SnapshotContext}.
 *
 * <p>A connector creates a new {@code DefaultSnapshotContext} when a session is
 * initialized or when recovery Phase A installs a new snapshot gatekeeper in
 * the parse context. The supplied gatekeeper is shared with that parse context,
 * and {@link #onSnapshotBoundaryAccepted()} routes the Phase B completion signal
 * back to connector lifecycle code.</p>
 */
public final class DefaultSnapshotContext implements SnapshotContext {
    private final SnapshotGatekeeper snapshotGatekeeper;
    private final SbeEncoder encoder;
    private final Publisher publisher;
    private final InstrumentCounters counters;
    private final NanoClock nanoClock;
    private final Runnable onSnapshotAccepted;
    private boolean boundaryAcceptedSignaled;

    /**
     * Creates a snapshot context for the current connector session.
     *
     * @param snapshotGatekeeper gatekeeper shared with the current parse context
     * @param encoder connector-owned encoder
     * @param publisher publisher used by encoder finalization
     * @param counters per-instrument counters
     * @param nanoClock connector-local nanosecond clock
     * @param onSnapshotAccepted callback invoked when the snapshot boundary is accepted
     * @throws NullPointerException if any argument is null
     */
    public DefaultSnapshotContext(
            SnapshotGatekeeper snapshotGatekeeper,
            SbeEncoder encoder,
            Publisher publisher,
            InstrumentCounters counters,
            NanoClock nanoClock,
            Runnable onSnapshotAccepted) {
        this.snapshotGatekeeper = Objects.requireNonNull(snapshotGatekeeper, "snapshotGatekeeper");
        this.encoder = Objects.requireNonNull(encoder, "encoder");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.onSnapshotAccepted = Objects.requireNonNull(onSnapshotAccepted, "onSnapshotAccepted");
    }

    @Override
    public SnapshotGatekeeper snapshotGatekeeper() {
        return snapshotGatekeeper;
    }

    @Override
    public SbeEncoder encoder() {
        return encoder;
    }

    @Override
    public Publisher publisher() {
        return publisher;
    }

    @Override
    public InstrumentCounters counters() {
        return counters;
    }

    @Override
    public NanoClock nanoClock() {
        return nanoClock;
    }

    /**
     * Routes snapshot boundary completion to the connector lifecycle callback exactly once.
     *
     * @throws IllegalStateException if this snapshot context has already signaled completion
     */
    @Override
    public void onSnapshotBoundaryAccepted() {
        if (boundaryAcceptedSignaled) {
            throw new IllegalStateException("Snapshot boundary was already accepted for this context");
        }
        boundaryAcceptedSignaled = true;
        onSnapshotAccepted.run();
    }
}
