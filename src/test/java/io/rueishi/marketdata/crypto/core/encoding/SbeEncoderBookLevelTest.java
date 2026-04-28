package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Byte-layout tests for {@link SbeEncoder} using the BOOK_LEVEL template.
 *
 * <p>The tests use a small capturing publisher rather than a production
 * publisher because P1-003 owns encoder correctness and the publisher
 * implementations arrive in later cards. Assertions decode captured bytes with
 * {@link SbeDecoder} to verify header, body, entry count, and level-entry
 * layout.</p>
 */
class SbeEncoderBookLevelTest {

    /**
     * Verifies a multi-entry BOOK_LEVEL message is encoded little-endian with the exact header/body fields.
     */
    @Test
    void encodesBookLevelMessageLittleEndian() {
        CapturingPublisher publisher = new CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(
                1001,
                VenueEnum.COINBASE_L2.byteValue(),
                BookDepth.L2.byteValue(),
                TemplateId.BOOK_LEVEL.byteValue(),
                10,
                0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT, 7, 11, 12, 123_456_789L, 987_654_321L);
        encoder.writeLevel((byte) 1, (byte) 1, 2_192_173L, (byte) 2, 125_000_000L, (byte) 8);
        encoder.writeLevel((byte) 2, (byte) 2, 2_192_200L, (byte) 2, 100_000_000L, (byte) 8);

        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isTrue();

        SbeDecoder decoded = new SbeDecoder(publisher.lastMessage());
        assertThat(decoded.magic()).isEqualTo(EncodingConstants.MAGIC);
        assertThat(decoded.version()).isEqualTo(EncodingConstants.VERSION);
        assertThat(decoded.templateId()).isEqualTo(TemplateId.BOOK_LEVEL.byteValue());
        assertThat(decoded.blockLength()).isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
        assertThat(decoded.entryCount()).isEqualTo(2);
        assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
        assertThat(decoded.venue()).isEqualTo(VenueEnum.COINBASE_L2.byteValue());
        assertThat(decoded.bookDepth()).isEqualTo(BookDepth.L2.byteValue());
        assertThat(decoded.instrumentId()).isEqualTo(1001);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(7);
        assertThat(decoded.seq1()).isEqualTo(11);
        assertThat(decoded.seq2()).isEqualTo(12);
        assertThat(decoded.exchangeTimestamp()).isEqualTo(123_456_789L);
        assertThat(decoded.ingressTimestamp()).isEqualTo(987_654_321L);
        assertThat(publisher.lastMessage().length).isEqualTo(
                EncodingConstants.REPEATING_GROUP_OFFSET + 2 * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH);
        assertThat(decoded.side(0)).isEqualTo(1);
        assertThat(decoded.action(0)).isEqualTo(1);
        assertThat(decoded.priceScale(0)).isEqualTo(2);
        assertThat(decoded.qtyScale(0)).isEqualTo(8);
        assertThat(decoded.priceMantissa(0)).isEqualTo(2_192_173L);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(125_000_000L);
        assertThat(publisher.lastLength()).isEqualTo(
                EncodingConstants.MESSAGE_PREFIX_LENGTH + 2 * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH);
    }

    /**
     * Verifies that an epoch-zero exchange timestamp is encoded as the required absent sentinel.
     */
    @Test
    void encodesEpochZeroExchangeTimestampAsAbsentSentinel() {
        CapturingPublisher publisher = new CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, 0L, 44L);
        encoder.endMessage(publisher, null, () -> 0L);

        assertThat(new SbeDecoder(publisher.lastMessage()).exchangeTimestamp()).isEqualTo(-1L);
    }

    /**
     * Verifies the encoded entryCount exactly matches zero, one, and larger BOOK_LEVEL messages.
     */
    @Test
    void patchesExactEntryCountForZeroOneAndManyEntries() {
        CapturingPublisher publisher = new CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(
                1001,
                VenueEnum.COINBASE_L2.byteValue(),
                BookDepth.L2.byteValue(),
                TemplateId.BOOK_LEVEL.byteValue(),
                4,
                0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 10L);
        encoder.endMessage(publisher, null, () -> 0L);
        assertThat(new SbeDecoder(publisher.lastMessage()).entryCount()).isZero();

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 2, 2, 2, -1L, 20L);
        encoder.writeLevel((byte) 1, (byte) 1, 100L, (byte) 1, 200L, (byte) 2);
        encoder.endMessage(publisher, null, () -> 0L);
        assertThat(new SbeDecoder(publisher.lastMessage()).entryCount()).isEqualTo(1);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 3, 3, 3, -1L, 30L);
        encoder.writeLevel((byte) 1, (byte) 1, 100L, (byte) 1, 200L, (byte) 2);
        encoder.writeLevel((byte) 2, (byte) 1, 101L, (byte) 1, 201L, (byte) 2);
        encoder.writeLevel((byte) 1, (byte) 2, 102L, (byte) 1, 0L, (byte) 2);
        encoder.endMessage(publisher, null, () -> 0L);
        assertThat(new SbeDecoder(publisher.lastMessage()).entryCount()).isEqualTo(3);
    }

    static final class CapturingPublisher implements Publisher {
        private byte[] lastMessage;
        private int lastLength;
        private boolean nextPublishResult = true;

        @Override
        public boolean publish(
                DirectBuffer buffer,
                int offset,
                int length,
                InstrumentCounters counters,
                NanoClock nanoClock) {
            lastMessage = new byte[length];
            buffer.getBytes(offset, lastMessage, 0, length);
            lastLength = length;
            return nextPublishResult;
        }

        @Override
        public void publishReset(
                int instrumentId,
                byte venueByte,
                byte bookDepthByte,
                byte templateIdByte,
                NanoClock nanoClock) {
            throw new UnsupportedOperationException("reset capture is not needed by P1-003 encoder tests");
        }

        byte[] lastMessage() {
            return lastMessage;
        }

        int lastLength() {
            return lastLength;
        }

        void nextPublishResult(boolean nextPublishResult) {
            this.nextPublishResult = nextPublishResult;
        }
    }
}
