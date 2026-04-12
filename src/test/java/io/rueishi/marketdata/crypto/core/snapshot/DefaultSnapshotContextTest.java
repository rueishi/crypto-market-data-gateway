package io.rueishi.marketdata.crypto.core.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultSnapshotContext}.
 *
 * <p>These tests verify the snapshot context's key lifecycle invariant: it must
 * share the same {@link SnapshotGatekeeper} currently installed in the parse
 * context after session reset. They also verify the one-shot completion callback
 * that later connector code uses to clear recovery-in-progress state.</p>
 */
class DefaultSnapshotContextTest {

    /**
     * Verifies that accepting the snapshot gate through snapshot context opens the parser gate immediately.
     */
    @Test
    void snapshotContextSharesGatekeeperWithParseContextAfterReset() {
        DefaultParseContext parseContext = newParseContext();
        parseContext.resetSession();

        DefaultSnapshotContext snapshotContext = new DefaultSnapshotContext(
                parseContext.currentSnapshotGatekeeper(),
                parseContext.encoder(),
                parseContext.publisher(),
                parseContext.counters(),
                parseContext.nanoClock(),
                () -> {
                });

        assertThat(snapshotContext.snapshotGatekeeper()).isSameAs(parseContext.snapshotGatekeeper());
        snapshotContext.snapshotGatekeeper().accept();
        assertThat(parseContext.snapshotGatekeeper().isReady()).isTrue();
    }

    /**
     * Verifies that snapshot boundary completion invokes the connector callback once.
     */
    @Test
    void snapshotBoundaryAcceptedCallbackIsOneShot() {
        DefaultParseContext parseContext = newParseContext();
        AtomicInteger acceptedCount = new AtomicInteger();
        DefaultSnapshotContext snapshotContext = new DefaultSnapshotContext(
                parseContext.currentSnapshotGatekeeper(),
                parseContext.encoder(),
                parseContext.publisher(),
                parseContext.counters(),
                parseContext.nanoClock(),
                acceptedCount::incrementAndGet);

        snapshotContext.snapshotGatekeeper().accept();
        snapshotContext.onSnapshotBoundaryAccepted();

        assertThat(acceptedCount).hasValue(1);
        assertThatThrownBy(snapshotContext::onSnapshotBoundaryAccepted)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already accepted");
    }

    private static DefaultParseContext newParseContext() {
        return new DefaultParseContext(
                CoreTestFixtures.newEncoder(),
                CoreTestFixtures.noopPublisher(),
                CoreTestFixtures.newInstrumentCounters(),
                () -> 123L);
    }
}
