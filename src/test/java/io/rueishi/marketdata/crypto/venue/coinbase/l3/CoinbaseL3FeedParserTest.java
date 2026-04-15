package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.Unpooled;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Fixture-driven tests for {@link CoinbaseL3FeedParser}.
 *
 * <p>The suite verifies normalization of Coinbase full-feed order lifecycle,
 * trade, heartbeat, subscriptions-acknowledgement, and error frames into the
 * shared schema v2 SBE layouts. It uses the production parser, production
 * encoders, real counters, and an in-memory publisher while stubbing only the
 * connector-owned recovery callback. Scenarios cover happy-path encoding,
 * product filtering, pre-snapshot drops, malformed-frame rejection, UUID and
 * decimal parsing edge cases, and subscriptions/error recovery triggers.</p>
 */
class CoinbaseL3FeedParserTest {
    /**
     * Verifies a received limit order publishes an ORDER_EVENT with the normalized received reason.
     */
    @Test
    void onTextFrame_received_limitOrder_reasonReceivedZero() {
        Support support = Support.live();
        support.parseFixture("received.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_RECEIVED);
    }

    @Test
    void onTextFrame_received_limitOrder_buyEncodesAsBid() {
        Support support = Support.live();
        support.parseFixture("received.json");

        assertThat(support.decoded().side()).isEqualTo(EncodingConstants.SIDE_BID);
    }

    @Test
    void onTextFrame_received_limitOrder_sellEncodesAsAsk() {
        Support support = Support.live();
        support.parseJson(support.fixture("received.json").replace("\"side\":\"buy\"", "\"side\":\"sell\""));

        assertThat(support.decoded().side()).isEqualTo(EncodingConstants.SIDE_ASK);
    }

    @Test
    void onTextFrame_received_orderTypeLimitEncoded() {
        Support support = Support.live();
        support.parseFixture("received.json");

        assertThat(support.decoded().orderType()).isEqualTo(EncodingConstants.ORDER_TYPE_LIMIT);
    }

    @Test
    void onTextFrame_received_orderTypeMarketEncoded() {
        Support support = Support.live();
        support.parseFixture("received_market_order.json");

        assertThat(support.decoded().orderType()).isEqualTo(EncodingConstants.ORDER_TYPE_MARKET);
    }

    @Test
    void onTextFrame_received_orderTypeStopEncoded() {
        Support support = Support.live();
        support.parseJson(support.fixture("received.json").replace("\"order_type\":\"limit\"", "\"order_type\":\"stop\""));

        assertThat(support.decoded().orderType()).isEqualTo(EncodingConstants.ORDER_TYPE_STOP);
    }

    @Test
    void onTextFrame_received_marketOrder_priceMantissaZero_qtyMantissaZero() {
        Support support = Support.live();
        support.parseFixture("received_market_order.json");

        assertThat(support.decoded().priceMantissa()).isZero();
        assertThat(support.decoded().priceScale()).isZero();
        assertThat(support.decoded().qtyMantissa()).isZero();
        assertThat(support.decoded().qtyScale()).isZero();
        assertThat(support.counters.malformedRejections().get()).isZero();
    }

    @Test
    void onTextFrame_received_fullUuidInOrderIdHighAndLow() {
        Support support = Support.live();
        support.parseFixture("received.json");

        assertThat(support.decoded().orderIdHigh()).isEqualTo(0xd50ec98477a8460aL);
        assertThat(support.decoded().orderIdLow()).isEqualTo(0xb95866f114b0de9bL);
    }

    @Test
    void onTextFrame_received_timestampFromTimeField() {
        Support support = Support.live();
        support.parseFixture("received.json");

        assertThat(support.decoded().exchangeTimestamp()).isEqualTo(1_675_974_789_123_456_000L);
    }

    /**
     * Verifies open messages normalize remaining size and the forced LIMIT order type.
     */
    @Test
    void onTextFrame_open_reasonOpenOne() {
        Support support = Support.live();
        support.parseFixture("open.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_OPEN);
    }

    @Test
    void onTextFrame_open_qtyFromRemainingSize() {
        Support support = Support.live();
        support.parseFixture("open.json");

        assertThat(support.decoded().qtyMantissa()).isEqualTo(100_000_000L);
        assertThat(support.decoded().qtyScale()).isEqualTo(8);
    }

    @Test
    void onTextFrame_open_orderTypeForcedToLimit() {
        Support support = Support.live();
        support.parseFixture("open.json");

        assertThat(support.decoded().orderType()).isEqualTo(EncodingConstants.ORDER_TYPE_LIMIT);
    }

    @Test
    void onTextFrame_done_filledReason_mapsToFilledFour() {
        Support support = Support.live();
        support.parseFixture("done_filled.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_FILLED);
    }

    @Test
    void onTextFrame_done_cancelledReason_mapsToCancelledFive() {
        Support support = Support.live();
        support.parseFixture("done_cancelled.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_CANCELLED);
    }

    @Test
    void onTextFrame_done_remainingSizeEncodedInQtyMantissa() {
        Support support = Support.live();
        support.parseFixture("done_cancelled.json");

        assertThat(support.decoded().qtyMantissa()).isEqualTo(75_000_000L);
        assertThat(support.decoded().qtyScale()).isEqualTo(8);
    }

    @Test
    void onTextFrame_done_missingPrice_priceMantissaZero() {
        Support support = Support.live();
        support.parseJson(support.fixture("done_filled.json").replace(",\"price\":\"333.98\"", ""));

        assertThat(support.decoded().priceMantissa()).isZero();
        assertThat(support.decoded().priceScale()).isZero();
    }

    /**
     * Verifies activate and change messages keep the L3 order-event fields required for downstream state changes.
     */
    @Test
    void onTextFrame_activate_reasonTriggeredTwo() {
        Support support = Support.live();
        support.parseFixture("activate.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_TRIGGERED);
    }

    @Test
    void onTextFrame_activate_orderTypeStop() {
        Support support = Support.live();
        support.parseFixture("activate.json");

        assertThat(support.decoded().orderType()).isEqualTo(EncodingConstants.ORDER_TYPE_STOP);
    }

    @Test
    void onTextFrame_activate_stopPriceEncoded() {
        Support support = Support.live();
        support.parseFixture("activate.json");

        assertThat(support.decoded().priceMantissa()).isEqualTo(33_500L);
        assertThat(support.decoded().priceScale()).isEqualTo(2);
    }

    @Test
    void onTextFrame_activate_qtyMantissaZero() {
        Support support = Support.live();
        support.parseFixture("activate.json");

        assertThat(support.decoded().qtyMantissa()).isZero();
        assertThat(support.decoded().qtyScale()).isZero();
    }

    @Test
    void onTextFrame_change_reasonModifiedThree() {
        Support support = Support.live();
        support.parseFixture("change.json");

        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_MODIFIED);
    }

    @Test
    void onTextFrame_change_newSizeInQtyMantissa() {
        Support support = Support.live();
        support.parseFixture("change.json");

        assertThat(support.decoded().qtyMantissa()).isEqualTo(523_512L);
        assertThat(support.decoded().qtyScale()).isEqualTo(5);
    }

    @Test
    void onTextFrame_change_oldSizeInOldQtyMantissa() {
        Support support = Support.live();
        support.parseFixture("change.json");

        assertThat(support.decoded().oldQtyMantissa()).isEqualTo(1_200_000L);
        assertThat(support.decoded().oldQtyScale()).isEqualTo(5);
    }

    @Test
    void onTextFrame_change_bothScalesEncoded() {
        Support support = Support.live();
        support.parseFixture("change.json");

        assertThat(support.decoded().priceScale()).isEqualTo(2);
        assertThat(support.decoded().qtyScale()).isEqualTo(5);
        assertThat(support.decoded().oldQtyScale()).isEqualTo(5);
    }

    /**
     * Verifies match messages switch to the trade template and preserve both 128-bit UUIDs.
     */
    @Test
    void onTextFrame_match_eventTypeIsBookTrade() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_TRADE);
    }

    @Test
    void onTextFrame_match_templateIdIsTradeEvent() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().templateId()).isEqualTo(EncodingConstants.TEMPLATE_ID_TRADE_EVENT);
    }

    @Test
    void onTextFrame_match_entryCountIsOne() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().entryCount()).isEqualTo(1);
    }

    @Test
    void onTextFrame_match_makerOrderIdHighAndLowFromMakerUuid() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().makerOrderIdHigh()).isEqualTo(0xac928c66ca53498fL);
        assertThat(support.decoded().makerOrderIdLow()).isEqualTo(0x9c13a110027a60e8L);
    }

    @Test
    void onTextFrame_match_takerOrderIdHighAndLowFromTakerUuid() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().takerOrderIdHigh()).isEqualTo(0x132fb6ae456b4654L);
        assertThat(support.decoded().takerOrderIdLow()).isEqualTo(0xb4e0d681ac05cea1L);
    }

    @Test
    void onTextFrame_match_sideIsTakerSide_sellEncodesAsAsk() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().tradeSide()).isEqualTo(EncodingConstants.SIDE_ASK);
    }

    @Test
    void onTextFrame_match_sideIsTakerSide_buyEncodesAsBid() {
        Support support = Support.live();
        support.parseJson(support.fixture("match.json").replace("\"side\":\"sell\"", "\"side\":\"buy\""));

        assertThat(support.decoded().tradeSide()).isEqualTo(EncodingConstants.SIDE_BID);
    }

    @Test
    void onTextFrame_match_priceAndQtyEncoded() {
        Support support = Support.live();
        support.parseFixture("match.json");

        assertThat(support.decoded().tradePriceMantissa()).isEqualTo(40_023L);
        assertThat(support.decoded().tradePriceScale()).isEqualTo(2);
        assertThat(support.decoded().tradeQtyMantissa()).isEqualTo(523_512L);
        assertThat(support.decoded().tradeQtyScale()).isEqualTo(5);
        assertThat(support.decoded().seq1()).isEqualTo(10_000_007L);
        assertThat(support.decoded().seq2()).isEqualTo(10_000_007L);
    }

    @Test
    void onTextFrame_heartbeat_updatesLastHeartbeatNanos() {
        Support support = Support.live();
        support.parseFixture("heartbeat.json");

        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
    }

    @Test
    void onTextFrame_heartbeat_incrementsCounter() {
        Support support = Support.live();
        support.parseFixture("heartbeat.json");

        assertThat(support.counters.heartbeatsReceived().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_heartbeat_doesNotPublish() {
        Support support = Support.live();
        support.parseFixture("heartbeat.json");

        assertThat(support.publisher.totalCount()).isZero();
    }

    @Test
    void onTextFrame_subscriptionsAck_bothChannelsPresent_passes() {
        Support support = Support.live();
        support.parseFixture("subscriptions_ack.json");

        assertThat(support.recoverySignals).isEmpty();
        assertThat(support.context.subscriptionAckValidated()).isTrue();
    }

    @Test
    void onTextFrame_subscriptionsAck_seedsLastHeartbeatNanos() {
        Support support = Support.live();
        support.parseFixture("subscriptions_ack.json");

        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
    }

    /**
     * Verifies wrong-product and pre-snapshot paths are counted and dropped without publishing.
     */
    @Test
    void onTextFrame_received_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("received.json");
    }

    @Test
    void onTextFrame_open_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("open.json");
    }

    @Test
    void onTextFrame_done_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("done_cancelled.json");
    }

    @Test
    void onTextFrame_activate_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("activate.json");
    }

    @Test
    void onTextFrame_match_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("match.json");
    }

    @Test
    void onTextFrame_change_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("change.json");
    }

    @Test
    void onTextFrame_heartbeat_wrongProductId_droppedAndCounted() {
        assertWrongProductDropped("heartbeat.json");
    }

    @Test
    void onTextFrame_anyMessage_beforeSnapshot_preSnapshotDropIncremented_noPublish() {
        Support support = Support.preSnapshot();
        support.parseFixture("done_before_snapshot.json");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.preSnapshotDrops().get()).isEqualTo(1);
        assertThat(support.recoverySignals).isEmpty();
    }

    @Test
    void onTextFrame_activate_encoded_notDropped() {
        Support support = Support.live();
        support.parseFixture("activate.json");

        assertThat(support.publisher.totalCount()).isEqualTo(1);
        assertThat(support.decoded().reason()).isEqualTo(EncodingConstants.REASON_TRIGGERED);
        assertThat(support.counters.unknownTypeDrops().get()).isZero();
    }

    @Test
    void onTextFrame_unknownType_countedAndDropped() {
        Support support = Support.live();
        support.parseFixture("unknown_type.json");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.unknownTypeDrops().get()).isEqualTo(1);
    }

    /**
     * Verifies malformed inputs and control-plane failures are rejected or recovered explicitly.
     */
    @Test
    void onTextFrame_malformedJson_incrementsMalformedRejections() {
        Support support = Support.live();
        support.parseFixture("malformed.json");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_badDecimalPrice_incrementsMalformedRejections_noPublish() {
        Support support = Support.live();
        support.parseFixture("bad_decimal.json");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_badDecimalSize_incrementsMalformedRejections_noPublish() {
        Support support = Support.live();
        support.parseJson(support.fixture("received.json").replace("\"size\":\"1.00000000\"", "\"size\":\"bad-size\""));

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_missingOrderId_incrementsMalformedRejections() {
        Support support = Support.live();
        support.parseJson(support.fixture("received.json")
                .replace("\"order_id\":\"d50ec984-77a8-460a-b958-66f114b0de9b\",", ""));

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_missingType_incrementsMalformedRejections() {
        Support support = Support.live();
        support.parseJson(support.fixture("received.json").replace("\"type\":\"received\",", ""));

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_emptyFrame_handledGracefully() {
        Support support = Support.live();
        support.parseJson("");

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.malformedRejections().get()).isEqualTo(1);
    }

    @Test
    void onTextFrame_errorMessage_incrementsAuthErrors_triggersRecovery() {
        Support support = Support.live();
        support.parseFixture("error_message.json");

        assertThat(support.counters.authenticationErrors().get()).isEqualTo(1);
        assertThat(support.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                });
    }

    @Test
    void onTextFrame_subscriptionsAck_missingFullChannel_triggersRecovery() {
        assertInvalidAckTriggersRecovery("subscriptions_ack_missing_full.json");
    }

    @Test
    void onTextFrame_subscriptionsAck_missingHeartbeatChannel_triggersRecovery() {
        assertInvalidAckTriggersRecovery("subscriptions_ack_missing_heartbeat.json");
    }

    @Test
    void onTextFrame_subscriptionsAck_wrongProduct_triggersRecovery() {
        assertInvalidAckTriggersRecovery("subscriptions_ack_wrong_product.json");
    }

    private static void assertWrongProductDropped(String fixtureName) {
        Support support = Support.live();
        support.parseJson(support.fixture(fixtureName).replace("BTC-USD", "ETH-USD"));

        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.counters.productIdMismatches().get()).isEqualTo(1);
        assertThat(support.counters.unknownSymbolDrops().get()).isEqualTo(1);
    }

    private static void assertInvalidAckTriggersRecovery(String fixtureName) {
        Support support = Support.live();
        support.parseFixture(fixtureName);

        assertThat(support.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(support.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                });
    }

    /**
     * Production-style test harness for one configured Coinbase L3 parser session.
     */
    static final class Support {
        private static final Path FIXTURE_ROOT = Path.of("src/test/resources/venue/coinbase/l3");

        final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        final InMemoryPublisher publisher = new InMemoryPublisher(16, 4096);
        final NanoClock clock = () -> 9_876_543_210L;
        final List<RecoverySignal> recoverySignals = new ArrayList<>();
        final DefaultParseContext context;
        final CoinbaseL3FeedParser parser;

        private Support(boolean live) {
            InstrumentConfig instrument = new InstrumentConfig();
            instrument.exchangeSymbol = "BTC-USD";
            instrument.instrumentId = 1001;

            SbeEncoder orderEncoder = new SbeEncoder(
                    instrument.instrumentId,
                    VenueEnum.COINBASE_L3.byteValue(),
                    BookDepth.L3.byteValue(),
                    EncodingConstants.TEMPLATE_ID_ORDER_EVENT,
                    1,
                    0);
            SbeEncoder tradeEncoder = new SbeEncoder(
                    instrument.instrumentId,
                    VenueEnum.COINBASE_L3.byteValue(),
                    BookDepth.L3.byteValue(),
                    EncodingConstants.TEMPLATE_ID_TRADE_EVENT,
                    1,
                    0);
            this.context = new DefaultParseContext(
                    orderEncoder,
                    tradeEncoder,
                    publisher,
                    counters,
                    clock,
                    (type, reason, diagnosticText) -> recoverySignals.add(new RecoverySignal(type, reason, diagnosticText)),
                    () -> {
                    },
                    () -> {
                    });
            if (live) {
                context.snapshotGatekeeper().accept();
            }
            this.parser = new CoinbaseL3FeedParser(instrument);
        }

        static Support live() {
            return new Support(true);
        }

        static Support preSnapshot() {
            return new Support(false);
        }

        String fixture(String name) {
            try {
                return Files.readString(FIXTURE_ROOT.resolve(name));
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to read fixture " + name, ex);
            }
        }

        void parseFixture(String name) {
            parseJson(fixture(name));
        }

        void parseJson(String json) {
            parser.onTextFrame(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8), context);
        }

        DecodedMessage decoded() {
            return new DecodedMessage(publisher.lastMessage());
        }
    }

    /**
     * Captured parser-originated recovery metadata.
     */
    record RecoverySignal(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText) {
    }

    /**
     * Small local decoder for schema v2 ORDER_EVENT and TRADE_EVENT assertions.
     */
    static final class DecodedMessage {
        private final ByteBuffer buffer;

        DecodedMessage(byte[] encoded) {
            this.buffer = ByteBuffer.wrap(encoded).order(EncodingConstants.BYTE_ORDER);
        }

        int templateId() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.TEMPLATE_ID_OFFSET));
        }

        int eventType() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.EVENT_TYPE_OFFSET));
        }

        long seq1() {
            return buffer.getLong(EncodingConstants.SEQ1_OFFSET);
        }

        long seq2() {
            return buffer.getLong(EncodingConstants.SEQ2_OFFSET);
        }

        long exchangeTimestamp() {
            return buffer.getLong(EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET);
        }

        int entryCount() {
            return Short.toUnsignedInt(buffer.getShort(EncodingConstants.ENTRY_COUNT_OFFSET));
        }

        long orderIdHigh() {
            return buffer.getLong(orderBase() + EncodingConstants.ORDER_EVENT_ORDER_ID_HIGH_OFFSET);
        }

        long orderIdLow() {
            return buffer.getLong(orderBase() + EncodingConstants.ORDER_EVENT_ORDER_ID_LOW_OFFSET);
        }

        int side() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_SIDE_OFFSET));
        }

        int reason() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_REASON_OFFSET));
        }

        int orderType() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_ORDER_TYPE_OFFSET));
        }

        int priceScale() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_PRICE_SCALE_OFFSET));
        }

        int qtyScale() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_QTY_SCALE_OFFSET));
        }

        int oldQtyScale() {
            return Byte.toUnsignedInt(buffer.get(orderBase() + EncodingConstants.ORDER_EVENT_OLD_QTY_SCALE_OFFSET));
        }

        long priceMantissa() {
            return buffer.getLong(orderBase() + EncodingConstants.ORDER_EVENT_PRICE_MANTISSA_OFFSET);
        }

        long qtyMantissa() {
            return buffer.getLong(orderBase() + EncodingConstants.ORDER_EVENT_QTY_MANTISSA_OFFSET);
        }

        long oldQtyMantissa() {
            return buffer.getLong(orderBase() + EncodingConstants.ORDER_EVENT_OLD_QTY_MANTISSA_OFFSET);
        }

        long makerOrderIdHigh() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_MAKER_ID_HIGH_OFFSET);
        }

        long makerOrderIdLow() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_MAKER_ID_LOW_OFFSET);
        }

        long takerOrderIdHigh() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_TAKER_ID_HIGH_OFFSET);
        }

        long takerOrderIdLow() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_TAKER_ID_LOW_OFFSET);
        }

        int tradeSide() {
            return Byte.toUnsignedInt(buffer.get(tradeBase() + EncodingConstants.TRADE_EVENT_SIDE_OFFSET));
        }

        int tradePriceScale() {
            return Byte.toUnsignedInt(buffer.get(tradeBase() + EncodingConstants.TRADE_EVENT_PRICE_SCALE_OFFSET));
        }

        int tradeQtyScale() {
            return Byte.toUnsignedInt(buffer.get(tradeBase() + EncodingConstants.TRADE_EVENT_QTY_SCALE_OFFSET));
        }

        long tradePriceMantissa() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_PRICE_MANTISSA_OFFSET);
        }

        long tradeQtyMantissa() {
            return buffer.getLong(tradeBase() + EncodingConstants.TRADE_EVENT_QTY_MANTISSA_OFFSET);
        }

        private static int orderBase() {
            return EncodingConstants.REPEATING_GROUP_OFFSET;
        }

        private static int tradeBase() {
            return EncodingConstants.REPEATING_GROUP_OFFSET;
        }
    }
}
