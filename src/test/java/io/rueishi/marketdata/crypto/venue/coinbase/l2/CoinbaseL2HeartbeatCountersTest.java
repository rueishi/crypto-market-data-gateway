package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Heartbeat counter tests for the Coinbase parser live path.
 *
 * <p>The suite wires the production Coinbase L2 parser test harness with
 * in-memory counters, encoder, and publisher. It verifies that heartbeat frames
 * update per-instrument liveness counters and timestamps without publishing
 * SBE book output.</p>
 */
class CoinbaseL2HeartbeatCountersTest {

    /**
     * Verifies heartbeat frames update counters and do not publish downstream messages.
     */
    @Test
    void heartbeatFrameUpdatesCountersAndDoesNotPublish() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"heartbeat","sequence":90,"product_id":"BTC-USD"}""");

        assertThat(support.counters.heartbeatsReceived().get()).isEqualTo(1);
        assertThat(support.counters.messagesDecoded().get()).isEqualTo(1);
        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
        assertThat(support.publisher.totalCount()).isZero();
    }
}
