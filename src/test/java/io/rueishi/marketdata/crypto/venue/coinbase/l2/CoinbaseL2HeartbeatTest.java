package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Heartbeat handling tests for {@link CoinbaseL2FeedParser}.
 *
 * <p>The parser updates liveness counters and timestamps for Coinbase
 * {@code heartbeat} frames. Transport scheduling and recovery-on-timeout remain
 * connector responsibilities outside this task card, so the suite verifies no
 * book message is published from heartbeat input.</p>
 */
class CoinbaseL2HeartbeatTest {

    /**
     * Verifies a matching heartbeat updates liveness state and does not publish SBE output.
     */
    @Test
    void heartbeatUpdatesCountersAndDoesNotPublish() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"heartbeat","sequence":90,"product_id":"BTC-USD",
                "time":"2014-11-07T08:19:28.464459Z"}""");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.heartbeatsReceived().get()).isEqualTo(1);
        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
        assertThat(support.counters.messagesDecoded().get()).isEqualTo(1);
    }

    /**
     * Verifies a heartbeat for another product is rejected as a product mismatch.
     */
    @Test
    void heartbeatProductMismatchIsDropped() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"heartbeat","sequence":90,"product_id":"ETH-USD"}""");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.heartbeatsReceived().get()).isZero();
        assertThat(support.counters.productIdMismatches().get()).isEqualTo(1);
    }
}
