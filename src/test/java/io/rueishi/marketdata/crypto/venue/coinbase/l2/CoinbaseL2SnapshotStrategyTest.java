package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Coinbase L2 subscribe-driven snapshot boundary.
 *
 * <p>The suite verifies the venue strategy class and the parser-owned boundary
 * behavior used by that strategy. It uses the production parser, encoder,
 * gatekeeper, and parse context while stubbing only the connector callback that
 * would normally clear recovery state after
 * {@code onSnapshotBoundaryAccepted()}.</p>
 */
class CoinbaseL2SnapshotStrategyTest {

    /**
     * Verifies the venue strategy advertises subscribe-driven mode and does not open the gate early.
     */
    @Test
    void triggerSnapshotIsSubscribeDrivenNoOp() {
        CoinbaseL2SnapshotStrategy strategy = new CoinbaseL2SnapshotStrategy();
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        strategy.triggerSnapshot(support.instrument, support.snapshotContext());

        assertThat(strategy.mode()).isEqualTo(SnapshotStrategy.Mode.SUBSCRIBE_DRIVEN);
        assertThat(support.context.snapshotGatekeeper().isReady()).isFalse();
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(0);
        assertThat(support.publisher.totalCount()).isZero();
    }

    /**
     * Verifies a valid feed snapshot publishes, opens the update gate, and completes the boundary once.
     */
    @Test
    void validSnapshotOpensGateAndSignalsBoundaryAccepted() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD",
                "bids":[["21921.73","0.06317902"]],"asks":[]}""");

        assertThat(support.context.snapshotGatekeeper().isReady()).isTrue();
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(1);
        assertThat(support.publisher.totalCount()).isEqualTo(1);
        assertThat(support.counters.bookSnapshotPublished().get()).isEqualTo(1);
    }

    /**
     * Verifies malformed snapshots are rejected without opening the gate or completing recovery Phase B.
     */
    @Test
    void invalidSnapshotDoesNotOpenGateOrSignalBoundaryAccepted() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD",
                "bids":[["21921.73","bad-size"]],"asks":[]}""");

        assertThat(support.context.snapshotGatekeeper().isReady()).isFalse();
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(0);
        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    /**
     * Verifies duplicate snapshots are ignored after the first boundary instead of corrupting gate state.
     */
    @Test
    void duplicateSnapshotAfterBoundaryDoesNotRepublishOrResignal() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();
        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD","bids":[],"asks":[]}""");

        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD","bids":[],"asks":[]}""");

        assertThat(support.context.snapshotGatekeeper().isReady()).isTrue();
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(1);
        assertThat(support.publisher.totalCount()).isEqualTo(1);
    }
}
