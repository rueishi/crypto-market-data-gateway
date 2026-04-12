package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.netty.buffer.Unpooled;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.snapshot.DefaultSnapshotContext;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.NanoClock;

/**
 * Test fixtures for Coinbase L2 parser suites.
 *
 * <p>The support object wires the production parser, context, encoder, and
 * in-memory publisher for one configured {@code BTC-USD} instrument. Tests use
 * this harness to verify parser behavior without introducing connector or
 * transport code. The harness records snapshot-boundary and recovery callbacks
 * that a real connector would receive from the parse context.</p>
 */
final class CoinbaseL2ParserTestSupport {
    final InstrumentConfig instrument = instrument();
    final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
    final InMemoryPublisher publisher = new InMemoryPublisher(8, 4096);
    final NanoClock clock = () -> 9_876_543_210L;
    final List<RecoverySignal> recoverySignals = new ArrayList<>();
    final AtomicInteger snapshotBoundaryAcceptedCount = new AtomicInteger();
    final DefaultParseContext context = new DefaultParseContext(
            new SbeEncoder(
                    instrument.instrumentId,
                    VenueEnum.COINBASE_L2.byteValue(),
                    BookDepth.L2.byteValue(),
                    TemplateId.BOOK_LEVEL.byteValue(),
                    16,
                    0),
            publisher,
            counters,
            clock,
            (type, reason, diagnosticText) -> recoverySignals.add(
                    new RecoverySignal(type, reason, diagnosticText)),
            () -> {
            },
            snapshotBoundaryAcceptedCount::incrementAndGet);
    final CoinbaseL2FeedParser parser = new CoinbaseL2FeedParser(instrument);

    /**
     * Parses one JSON fixture string as a Netty text frame.
     *
     * @param json Coinbase JSON fixture
     */
    void parse(String json) {
        parser.onTextFrame(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8), context);
    }

    /**
     * Captured parser-originated recovery request metadata.
     *
     * @param type requested recovery action
     * @param reason informational recovery reason
     * @param diagnosticText optional diagnostic text supplied by the parser
     */
    record RecoverySignal(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText) {
    }

    /**
     * Creates a snapshot context sharing the parser gate for strategy tests.
     *
     * @return snapshot context backed by this support object's current parser session
     */
    DefaultSnapshotContext snapshotContext() {
        return new DefaultSnapshotContext(
                context.currentSnapshotGatekeeper(),
                context.encoder(),
                context.publisher(),
                context.counters(),
                context.nanoClock(),
                snapshotBoundaryAcceptedCount::incrementAndGet);
    }

    private static InstrumentConfig instrument() {
        InstrumentConfig config = new InstrumentConfig();
        config.exchangeSymbol = "BTC-USD";
        config.instrumentId = 1001;
        return config;
    }

    /**
     * Test-only decoder for captured BOOK_LEVEL messages.
     *
     * <p>This local decoder mirrors the core test decoder without widening that
     * package-private helper's visibility across test packages.</p>
     */
    static final class DecodedMessage {
        private final ByteBuffer buffer;

        /**
         * Wraps one encoded message captured by {@link InMemoryPublisher}.
         *
         * @param encoded encoded gateway message bytes
         */
        DecodedMessage(byte[] encoded) {
            this.buffer = ByteBuffer.wrap(encoded).order(EncodingConstants.BYTE_ORDER);
        }

        int eventType() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.EVENT_TYPE_OFFSET));
        }

        int venue() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.VENUE_OFFSET));
        }

        int bookDepth() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.BOOK_DEPTH_OFFSET));
        }

        long instrumentId() {
            return Integer.toUnsignedLong(buffer.getInt(EncodingConstants.INSTRUMENT_ID_OFFSET));
        }

        long gatewayMessageSeq() {
            return buffer.getLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET);
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

        long ingressTimestamp() {
            return buffer.getLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET);
        }

        int entryCount() {
            return Short.toUnsignedInt(buffer.getShort(EncodingConstants.ENTRY_COUNT_OFFSET));
        }

        int side(int index) {
            return Byte.toUnsignedInt(buffer.get(levelOffset(index)));
        }

        int action(int index) {
            return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 1));
        }

        int priceScale(int index) {
            return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 2));
        }

        int qtyScale(int index) {
            return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 3));
        }

        long priceMantissa(int index) {
            return buffer.getLong(levelOffset(index) + 4);
        }

        long qtyMantissa(int index) {
            return buffer.getLong(levelOffset(index) + 12);
        }

        private int levelOffset(int index) {
            return EncodingConstants.MESSAGE_PREFIX_LENGTH + index * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH;
        }
    }
}
