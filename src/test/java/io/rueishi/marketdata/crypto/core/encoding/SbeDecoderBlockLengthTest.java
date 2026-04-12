package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
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
    void decoderUsesBlockLengthToFindRepeatingGroup() {
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
}
