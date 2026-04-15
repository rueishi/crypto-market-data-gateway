package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Decoder-location tests for the test-only {@link SbeDecoder}.
 *
 * <p>The decoder must locate repeating-group entries from the encoded
 * {@code blockLength} header rather than a hard-coded body offset. This keeps
 * tests aligned with the consumer rule in the binary layout spec.</p>
 */
class SbeDecoderBlockLengthTest {

    /**
     * Verifies that level entry decoding begins at {@code HEADER_LENGTH + decoded blockLength}.
     */
    @Test
    void decoder_usesBlockLength51_toLocateRepeatingGroup() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT, 1, 1, 1, -1L, 1L);
        encoder.writeLevel((byte) 1, (byte) 2, 123L, (byte) 4, 567L, (byte) 8);
        encoder.endMessage(publisher, null, () -> 0L);

        SbeDecoder decoded = new SbeDecoder(publisher.lastMessage());

        assertThat(decoded.blockLength()).isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
        assertThat(decoded.priceMantissa(0)).isEqualTo(123L);
        assertThat(decoded.qtyMantissa(0)).isEqualTo(567L);
    }

    /**
     * Verifies schema v2 messages carry version byte 2 so downstream decoders can reject stale v1 payloads.
     */
    @Test
    void decoder_version2_acceptedByDecoder() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT, 1, 1, 1, -1L, 1L);
        encoder.endMessage(publisher, null, () -> 0L);

        assertThat(new SbeDecoder(publisher.lastMessage()).version()).isEqualTo(EncodingConstants.VERSION);
        assertThat(EncodingConstants.VERSION).isEqualTo(2);
    }

    /**
     * Verifies a zero-entry BOOK_RESET-shaped message ends at the schema v2 59-byte body boundary.
     */
    @Test
    void decoder_bookReset_totalSizeIs59Bytes() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_RESET, 1, 1, 1, -1L, 1L);
        encoder.endMessage(publisher, null, () -> 0L);

        SbeDecoder decoded = new SbeDecoder(publisher.lastMessage());
        assertThat(publisher.lastLength()).isEqualTo(EncodingConstants.BOOK_RESET_SIZE);
        assertThat(publisher.lastLength()).isEqualTo(59);
        assertThat(decoded.entryCount()).isZero();
    }

    /**
     * Verifies the new schema v2 checksum field is addressable at offset 55 and currently encodes as zero.
     */
    @Test
    void decoder_checksum_readableAtOffset55() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_RESET, 1, 1, 1, -1L, 1L);
        encoder.endMessage(publisher, null, () -> 0L);

        int checksum = ByteBuffer.wrap(publisher.lastMessage())
                .order(EncodingConstants.BYTE_ORDER)
                .getInt(EncodingConstants.CHECKSUM_OFFSET);
        assertThat(checksum).isEqualTo(EncodingConstants.NO_CHECKSUM);
        assertThat(EncodingConstants.CHECKSUM_OFFSET).isEqualTo(55);
    }
}
