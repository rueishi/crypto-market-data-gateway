package io.rueishi.marketdata.crypto.core.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SubscribeDrivenSnapshotStrategy}.
 *
 * <p>These tests cover the base strategy used by venues whose snapshots arrive
 * through the subscribed feed. The test uses real Phase 1 context objects with
 * no mocked dependencies so it can verify that calling
 * {@link SubscribeDrivenSnapshotStrategy#triggerSnapshot(InstrumentConfig, SnapshotContext)}
 * does not open the snapshot gate or complete the snapshot boundary early.</p>
 */
class SubscribeDrivenSnapshotStrategyTest {

    /**
     * Verifies that subscribe-driven mode is reported and triggerSnapshot leaves snapshot state untouched.
     */
    @Test
    void triggerSnapshotIsNoOpForSubscribeDrivenMode() {
        SubscribeDrivenSnapshotStrategy strategy = new TestSubscribeDrivenSnapshotStrategy();
        DefaultParseContext parseContext = newParseContext();
        AtomicInteger snapshotAcceptedCount = new AtomicInteger();
        DefaultSnapshotContext snapshotContext = new DefaultSnapshotContext(
                parseContext.currentSnapshotGatekeeper(),
                parseContext.encoder(),
                parseContext.publisher(),
                parseContext.counters(),
                parseContext.nanoClock(),
                snapshotAcceptedCount::incrementAndGet);

        strategy.triggerSnapshot(newInstrument(), snapshotContext);

        assertThat(strategy.mode()).isEqualTo(SnapshotStrategy.Mode.SUBSCRIBE_DRIVEN);
        assertThat(parseContext.snapshotGatekeeper().isReady()).isFalse();
        assertThat(snapshotAcceptedCount).hasValue(0);
    }

    private static DefaultParseContext newParseContext() {
        return new DefaultParseContext(
                CoreTestFixtures.newEncoder(),
                CoreTestFixtures.noopPublisher(),
                CoreTestFixtures.newInstrumentCounters(),
                () -> 123L);
    }

    private static InstrumentConfig newInstrument() {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = "BTC-USD";
        instrument.instrumentId = 1001;
        return instrument;
    }

    private static final class TestSubscribeDrivenSnapshotStrategy extends SubscribeDrivenSnapshotStrategy {
    }
}
