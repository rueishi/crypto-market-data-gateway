package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Fixture-driven verification suite for the Phase 2 Coinbase L2 parser.
 *
 * <p>The suite reads the JSON resources under {@code venue/coinbase/l2} and
 * drives the production {@link CoinbaseL2FeedParser} through
 * {@link CoinbaseL2ParserTestSupport}. It validates the supported Coinbase
 * snapshot, l2update, heartbeat, and subscriptions acknowledgement paths, plus
 * malformed, unknown-type, product-mismatch, pre-snapshot, and invalid
 * acknowledgement drop behavior. The connector and transport layers are
 * stubbed out by the parser support fixture while the real encoder, counters,
 * snapshot gate, and {@code InMemoryPublisher} are used.</p>
 */
class CoinbaseL2FixtureSuiteTest {
    private static final Path FIXTURE_DIR = Path.of("src/test/resources/venue/coinbase/l2");

    /**
     * Verifies the fixture directory contains the required Phase 2 Coinbase L2 coverage set.
     *
     * @throws Exception if fixture resource discovery fails
     */
    @Test
    void fixtureDirectoryContainsRequiredPhaseTwoCases() throws Exception {
        Set<String> fixtureNames;
        try (var files = Files.list(FIXTURE_DIR)) {
            fixtureNames = files
                    .filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toSet());
        }

        assertThat(fixtureNames).contains(
                "snapshot.json",
                "snapshot_without_time.json",
                "snapshot_epoch_zero_time.json",
                "snapshot_wrong_product.json",
                "l2update.json",
                "l2update_buy_upsert.json",
                "l2update_sell_delete.json",
                "l2update_multi_change.json",
                "l2update_before_snapshot.json",
                "l2update_wrong_product.json",
                "heartbeat.json",
                "heartbeat_wrong_product.json",
                "subscriptions_ack.json",
                "subscriptions_ack_extra_channel.json",
                "subscriptions_ack_missing_level2.json",
                "subscriptions_ack_missing_heartbeat.json",
                "subscriptions_ack_wrong_product.json",
                "bad_decimal.json",
                "malformed.json",
                "unknown_type.json",
                "product_mismatch.json");
    }

    /**
     * Verifies the snapshot fixture publishes a byte-level BOOK_SNAPSHOT with Coinbase L2 headers and entries.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void snapshotFixturePublishesBookSnapshot() throws Exception {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(fixture("snapshot.json"));

        CoinbaseL2ParserTestSupport.DecodedMessage decoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(support.publisher.lastMessage());
        assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
        assertThat(decoded.venue()).isEqualTo(VenueEnum.COINBASE_L2.byteValue());
        assertThat(decoded.bookDepth()).isEqualTo(BookDepth.L2.byteValue());
        assertThat(headerByte(support.publisher.lastMessage(), EncodingConstants.TEMPLATE_ID_OFFSET))
                .isEqualTo(TemplateId.BOOK_LEVEL.byteValue());
        assertThat(decoded.instrumentId()).isEqualTo(1001);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(1);
        assertThat(decoded.seq1()).isEqualTo(1);
        assertThat(decoded.seq2()).isEqualTo(1);
        assertThat(decoded.exchangeTimestamp()).isEqualTo(1_675_974_789_999_999_000L);
        assertThat(decoded.entryCount()).isEqualTo(2);
        assertThat(decoded.side(0)).isEqualTo(1);
        assertThat(decoded.action(0)).isEqualTo(1);
        assertThat(decoded.priceMantissa(0)).isEqualTo(2_192_173L);
        assertThat(decoded.priceScale(0)).isEqualTo(2);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(6_317_902L);
        assertThat(decoded.qtyScale(0)).isEqualTo(8);
        assertThat(decoded.side(1)).isEqualTo(2);
        assertThat(decoded.action(1)).isEqualTo(1);
        assertThat(support.snapshotBoundaryAcceptedCount).hasValue(1);
    }

    /**
     * Verifies the update fixture only publishes after the fixture snapshot opens the session gate.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void l2UpdateFixturePublishesBookUpdateAfterSnapshot() throws Exception {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(fixture("snapshot.json"));
        support.parse(fixture("l2update.json"));

        CoinbaseL2ParserTestSupport.DecodedMessage decoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(support.publisher.lastMessage());
        assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(2);
        assertThat(decoded.seq1()).isEqualTo(2);
        assertThat(decoded.seq2()).isEqualTo(2);
        assertThat(decoded.exchangeTimestamp()).isEqualTo(1_675_974_790_000_000_000L);
        assertThat(decoded.entryCount()).isEqualTo(2);
        assertThat(decoded.side(0)).isEqualTo(1);
        assertThat(decoded.action(0)).isEqualTo(1);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(5_000_000L);
        assertThat(decoded.side(1)).isEqualTo(2);
        assertThat(decoded.action(1)).isEqualTo(2);
        assertThat(decoded.qtyMantissa(1)).isZero();
    }

    /**
     * Verifies the expanded update fixtures publish exact entry counts and actions.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void expandedUpdateFixturesPublishExpectedEntriesAfterSnapshot() throws Exception {
        CoinbaseL2ParserTestSupport buy = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport sellDelete = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport multi = new CoinbaseL2ParserTestSupport();

        buy.parse(fixture("snapshot.json"));
        buy.parse(fixture("l2update_buy_upsert.json"));
        sellDelete.parse(fixture("snapshot.json"));
        sellDelete.parse(fixture("l2update_sell_delete.json"));
        multi.parse(fixture("snapshot.json"));
        multi.parse(fixture("l2update_multi_change.json"));

        CoinbaseL2ParserTestSupport.DecodedMessage buyDecoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(buy.publisher.lastMessage());
        CoinbaseL2ParserTestSupport.DecodedMessage sellDecoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(sellDelete.publisher.lastMessage());
        CoinbaseL2ParserTestSupport.DecodedMessage multiDecoded =
                new CoinbaseL2ParserTestSupport.DecodedMessage(multi.publisher.lastMessage());
        assertThat(buyDecoded.entryCount()).isEqualTo(1);
        assertThat(buyDecoded.side(0)).isEqualTo(1);
        assertThat(buyDecoded.action(0)).isEqualTo(1);
        assertThat(sellDecoded.entryCount()).isEqualTo(1);
        assertThat(sellDecoded.side(0)).isEqualTo(2);
        assertThat(sellDecoded.action(0)).isEqualTo(2);
        assertThat(multiDecoded.entryCount()).isEqualTo(3);
        assertThat(multiDecoded.action(1)).isEqualTo(2);
    }

    /**
     * Verifies snapshots without usable exchange timestamps encode the timestamp sentinel.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void snapshotTimestampEdgeFixturesEncodeSentinel() throws Exception {
        CoinbaseL2ParserTestSupport withoutTime = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport epochZero = new CoinbaseL2ParserTestSupport();

        withoutTime.parse(fixture("snapshot_without_time.json"));
        epochZero.parse(fixture("snapshot_epoch_zero_time.json"));

        assertThat(new CoinbaseL2ParserTestSupport.DecodedMessage(
                withoutTime.publisher.lastMessage()).exchangeTimestamp()).isEqualTo(-1L);
        assertThat(new CoinbaseL2ParserTestSupport.DecodedMessage(
                epochZero.publisher.lastMessage()).exchangeTimestamp()).isEqualTo(-1L);
    }

    /**
     * Verifies the subscriptions acknowledgement fixture starts ack state without publishing market data.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void subscriptionsAckFixtureValidatesControlPlaneOnly() throws Exception {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(fixture("subscriptions_ack.json"));

        assertThat(support.context.subscriptionAckValidated()).isTrue();
        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
        assertThat(support.counters.subscriptionValidationFailures().get()).isZero();
    }

    /**
     * Verifies acknowledgement edge fixtures accept extra channels and reject
     * missing or wrong-product required channels through recovery signaling.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void subscriptionAcknowledgementEdgeFixturesValidateRequiredChannels() throws Exception {
        CoinbaseL2ParserTestSupport extra = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport missingLevel2 = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport missingHeartbeat = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport wrongProduct = new CoinbaseL2ParserTestSupport();

        extra.parse(fixture("subscriptions_ack_extra_channel.json"));
        missingLevel2.parse(fixture("subscriptions_ack_missing_level2.json"));
        missingHeartbeat.parse(fixture("subscriptions_ack_missing_heartbeat.json"));
        wrongProduct.parse(fixture("subscriptions_ack_wrong_product.json"));

        assertThat(extra.context.subscriptionAckValidated()).isTrue();
        assertThat(extra.counters.subscriptionValidationFailures().get()).isZero();
        assertThat(missingLevel2.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(missingHeartbeat.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(wrongProduct.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(missingLevel2.recoverySignals).hasSize(1);
        assertThat(missingHeartbeat.recoverySignals).hasSize(1);
        assertThat(wrongProduct.recoverySignals).hasSize(1);
    }

    /**
     * Verifies the heartbeat fixture updates liveness counters without producing encoded book output.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void heartbeatFixtureUpdatesCountersWithoutPublishing() throws Exception {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(fixture("heartbeat.json"));

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.heartbeatsReceived().get()).isEqualTo(1);
        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
    }

    /**
     * Verifies malformed, unknown-type, product-mismatch, and pre-snapshot
     * anomaly fixtures are counted and dropped.
     *
     * @throws Exception if fixture reading fails
     */
    @Test
    void dropFixturesAreCountedWithoutPublishing() throws Exception {
        CoinbaseL2ParserTestSupport malformed = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport badDecimal = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport unknownType = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport productMismatch = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport snapshotWrongProduct = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport updateWrongProduct = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport heartbeatWrongProduct = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport preSnapshot = new CoinbaseL2ParserTestSupport();

        malformed.parse(fixture("malformed.json"));
        badDecimal.parse(fixture("bad_decimal.json"));
        unknownType.parse(fixture("unknown_type.json"));
        productMismatch.parse(fixture("product_mismatch.json"));
        snapshotWrongProduct.parse(fixture("snapshot_wrong_product.json"));
        updateWrongProduct.parse(fixture("l2update_wrong_product.json"));
        heartbeatWrongProduct.parse(fixture("heartbeat_wrong_product.json"));
        preSnapshot.parse(fixture("l2update_before_snapshot.json"));

        assertThat(malformed.publisher.totalCount()).isZero();
        assertThat(malformed.counters.malformedRejections().get()).isEqualTo(1);
        assertThat(badDecimal.publisher.totalCount()).isZero();
        assertThat(badDecimal.counters.malformedRejections().get()).isEqualTo(1);
        assertThat(unknownType.publisher.totalCount()).isZero();
        assertThat(unknownType.counters.unknownTypeDrops().get()).isEqualTo(1);
        assertThat(productMismatch.publisher.totalCount()).isZero();
        assertThat(productMismatch.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(productMismatch.counters.unknownSymbolDrops().get()).isEqualTo(1);
        assertThat(snapshotWrongProduct.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(updateWrongProduct.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(heartbeatWrongProduct.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(preSnapshot.publisher.totalCount()).isZero();
        assertThat(preSnapshot.counters.preSnapshotDrops().get()).isEqualTo(1);
    }

    private static String fixture(String name) throws java.io.IOException {
        return Files.readString(FIXTURE_DIR.resolve(name));
    }

    private static int headerByte(byte[] message, int offset) {
        return Byte.toUnsignedInt(message[offset]);
    }
}
