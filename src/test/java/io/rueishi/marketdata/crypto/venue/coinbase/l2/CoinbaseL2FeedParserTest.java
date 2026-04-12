package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import org.junit.jupiter.api.Test;

/**
 * Fixture-driven tests for {@link CoinbaseL2FeedParser} book-data parsing.
 *
 * <p>The suite covers Coinbase L2 {@code snapshot} and {@code l2update}
 * normalization into the shared BOOK_LEVEL binary layout, the snapshot
 * boundary signal emitted for subscribe-driven Coinbase sessions, and Phase 3
 * parser-originated recovery triggers for invalid feed transitions. It also
 * covers the Coinbase ordering anomaly case where a pre-snapshot update is
 * counted and dropped without recovery. The tests use the real parser,
 * encoder, counters, and in-memory publisher while stubbing only connector
 * callbacks.</p>
 */
class CoinbaseL2FeedParserTest {

    /**
     * Verifies that a Coinbase snapshot becomes a BOOK_SNAPSHOT, opens the update gate, and signals the boundary.
     */
    @Test
    void parsesSnapshotAndPublishesBookSnapshot() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD","time":"2023-02-09T20:33:09.999999Z",
                "bids":[["21921.73","0.06317902"]],"asks":[["21922.10","0.42000000"]]}""");

        CoinbaseL2ParserTestSupport.DecodedMessage decoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(support.publisher.lastMessage());
        assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
        assertThat(decoded.venue()).isEqualTo(VenueEnum.COINBASE_L2.byteValue());
        assertThat(decoded.bookDepth()).isEqualTo(BookDepth.L2.byteValue());
        assertThat(decoded.instrumentId()).isEqualTo(1001);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(1);
        assertThat(decoded.seq1()).isEqualTo(1);
        assertThat(decoded.seq2()).isEqualTo(1);
        assertThat(decoded.exchangeTimestamp()).isEqualTo(1_675_974_789_999_999_000L);
        assertThat(decoded.ingressTimestamp()).isEqualTo(9_876_543_210L);
        assertThat(decoded.entryCount()).isEqualTo(2);
        assertThat(decoded.side(0)).isEqualTo(1);
        assertThat(decoded.action(0)).isEqualTo(1);
        assertThat(decoded.priceMantissa(0)).isEqualTo(2_192_173L);
        assertThat(decoded.priceScale(0)).isEqualTo(2);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(6_317_902L);
        assertThat(decoded.qtyScale(0)).isEqualTo(8);
        assertThat(decoded.side(1)).isEqualTo(2);
        assertThat(support.context.snapshotGatekeeper().isReady()).isTrue();
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(1);
        assertThat(support.counters.bookSnapshotPublished().get()).isEqualTo(1);
        assertThat(support.recoverySignals).isEmpty();
    }

    /**
     * Verifies l2update action mapping and sequence assignment after a snapshot.
     */
    @Test
    void parsesL2UpdateAfterSnapshotWithUpsertAndDeleteActions() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();
        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD",
                "bids":[["21921.73","0.06317902"]],"asks":[]}""");

        support.parse("""
                {"type":"l2update","product_id":"BTC-USD","time":"2023-02-09T20:33:10Z",
                "changes":[["buy","21921.73","0.05000000"],["sell","21922.10","0"]]}""");

        CoinbaseL2ParserTestSupport.DecodedMessage decoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(support.publisher.lastMessage());
        assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(2);
        assertThat(decoded.seq1()).isEqualTo(2);
        assertThat(decoded.seq2()).isEqualTo(2);
        assertThat(decoded.entryCount()).isEqualTo(2);
        assertThat(decoded.side(0)).isEqualTo(1);
        assertThat(decoded.action(0)).isEqualTo(1);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(5_000_000L);
        assertThat(decoded.side(1)).isEqualTo(2);
        assertThat(decoded.action(1)).isEqualTo(2);
        assertThat(decoded.qtyMantissa(1)).isZero();
        assertThat(support.counters.updateMessagesReceived().get()).isEqualTo(1);
        assertThat(support.recoverySignals).isEmpty();
    }

    /**
     * Verifies that pre-snapshot updates and product mismatches are counted and
     * dropped without publishing. Coinbase guarantees channel ordering, so a
     * pre-snapshot update is treated as a technical anomaly rather than a
     * recovery trigger.
     */
    @Test
    void preSnapshotUpdateAndProductMismatchDoNotPublish() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"l2update","product_id":"BTC-USD",
                "changes":[["buy","21921.73","0.05000000"]]}""");
        support.parse("""
                {"type":"snapshot","product_id":"ETH-USD","bids":[],"asks":[]}""");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.preSnapshotDrops().get()).isEqualTo(1);
        assertThat(support.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(support.recoverySignals).isEmpty();
    }

    /**
     * Verifies duplicate snapshots are treated as invalid transitions and do not republish stale state.
     */
    @Test
    void duplicateSnapshotRequestsRecoveryWithoutPublishingAgain() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();
        String snapshot = """
                {"type":"snapshot","product_id":"BTC-USD",
                "bids":[["21921.73","0.06317902"]],"asks":[]}""";

        support.parse(snapshot);
        support.parse(snapshot);

        assertThat(support.publisher.totalCount()).isEqualTo(1);
        assertThat(support.counters.bookSnapshotPublished().get()).isEqualTo(1);
        assertThat(support.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.OUT_OF_ORDER_OR_INVALID_TRANSITION);
                    assertThat(signal.diagnosticText()).contains("duplicate snapshot");
                });
    }

    /**
     * Verifies that unknown message types are counted and ignored.
     */
    @Test
    void dropsUnknownType() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(""" 
                {"type":"ticker","product_id":"BTC-USD"}""");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.unknownTypeDrops().get()).isEqualTo(1);
    }

    /**
     * Verifies malformed decimal content is rejected before any partial message is published.
     */
    @Test
    void rejectsMalformedSnapshotWithoutPublishingPartialMessage() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"snapshot","product_id":"BTC-USD",
                "bids":[["21921.73","bad-size"]],"asks":[]}""");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.context.snapshotGatekeeper().isReady()).isFalse();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
        assertThat(support.context.sequenceTracker().current()).isZero();
    }
}
