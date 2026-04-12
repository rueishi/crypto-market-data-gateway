package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SbeRecoveryRequestDecoder}.
 *
 * <p>The suite uses the production {@link SbeRecoveryRequestEncoder} to create
 * valid control messages, then mutates selected fields to verify decoder
 * validation. Covered scenarios include single-request round trip, batch
 * expansion, malformed header rejection, invalid enum rejection, truncated
 * payload rejection, and rejection of market-data templates such as
 * `BOOK_RESET`.</p>
 */
class SbeRecoveryRequestDecoderTest {

    /**
     * Verifies a single request round-trips through the dedicated recovery control codec.
     */
    @Test
    void decodesSingleRecoveryRequest() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        RecoveryRequest original = new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESNAPSHOT,
                RecoveryReasonCode.MANUAL_RESET,
                123_456L,
                "operator");
        int length = new SbeRecoveryRequestEncoder().encodeRequest(buffer, 0, original);

        List<RecoveryRequest> decoded = new SbeRecoveryRequestDecoder().decode(buffer, 0, length);

        assertThat(decoded).hasSize(1);
        RecoveryRequest request = decoded.get(0);
        assertThat(request.venue).isEqualTo(original.venue);
        assertThat(request.instrumentId).isEqualTo(original.instrumentId);
        assertThat(request.requestType).isEqualTo(original.requestType);
        assertThat(request.reasonCode).isEqualTo(original.reasonCode);
        assertThat(request.requestTimestamp).isEqualTo(original.requestTimestamp);
        assertThat(request.diagnosticText).isEqualTo(original.diagnosticText);
    }

    /**
     * Verifies a batch request expands to one immutable request per instrument.
     */
    @Test
    void decodesBatchRecoveryRequest() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        int length = new SbeRecoveryRequestEncoder().encodeBatch(
                buffer,
                0,
                VenueEnum.COINBASE_L2,
                RecoveryRequestType.RESYNC,
                RecoveryReasonCode.CHECK_FAILED,
                999L,
                new int[] {1001, 1002, 1003});

        List<RecoveryRequest> decoded = new SbeRecoveryRequestDecoder().decode(buffer, 0, length);

        assertThat(decoded).hasSize(3);
        assertThat(decoded).extracting(request -> request.instrumentId)
                .containsExactly(1001, 1002, 1003);
        assertThat(decoded).allSatisfy(request -> {
            assertThat(request.venue).isEqualTo(VenueEnum.COINBASE_L2);
            assertThat(request.requestType).isEqualTo(RecoveryRequestType.RESYNC);
            assertThat(request.reasonCode).isEqualTo(RecoveryReasonCode.CHECK_FAILED);
            assertThat(request.requestTimestamp).isEqualTo(999L);
        });
    }

    /**
     * Verifies malformed or unsupported control messages are rejected before routing.
     */
    @Test
    void rejectsMalformedControlMessages() {
        SbeRecoveryRequestEncoder encoder = new SbeRecoveryRequestEncoder();
        SbeRecoveryRequestDecoder decoder = new SbeRecoveryRequestDecoder();
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        int length = encoder.encodeRequest(buffer, 0, new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L));

        buffer.putByte(EncodingConstants.VERSION_OFFSET, (byte) 99);
        int invalidVersionLength = length;
        assertThatThrownBy(() -> decoder.decode(buffer, 0, invalidVersionLength))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");

        length = encoder.encodeRequest(buffer, 0, new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L));
        buffer.putByte(EncodingConstants.HEADER_LENGTH + SbeRecoveryRequestEncoder.REASON_CODE_RELATIVE_OFFSET, (byte) 99);
        int invalidReasonLength = length;
        assertThatThrownBy(() -> decoder.decode(buffer, 0, invalidReasonLength))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");

        length = encoder.encodeBatch(
                buffer,
                0,
                VenueEnum.COINBASE_L2,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L,
                new int[] {1001});
        int truncatedLength = length - 1;
        assertThatThrownBy(() -> decoder.decode(buffer, 0, truncatedLength))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }

    /**
     * Verifies a gateway-to-downstream BOOK_RESET message is not accepted as a recovery request.
     */
    @Test
    void rejectsMarketDataBookResetTemplate() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[EncodingConstants.MESSAGE_PREFIX_LENGTH]);
        buffer.putShort(EncodingConstants.MAGIC_OFFSET, (short) EncodingConstants.MAGIC, EncodingConstants.BYTE_ORDER);
        buffer.putByte(EncodingConstants.VERSION_OFFSET, (byte) EncodingConstants.VERSION);
        buffer.putByte(EncodingConstants.TEMPLATE_ID_OFFSET, VenueEnum.COINBASE_L2.templateId().byteValue());
        buffer.putShort(
                EncodingConstants.BLOCK_LENGTH_OFFSET,
                (short) EncodingConstants.BODY_BLOCK_LENGTH,
                EncodingConstants.BYTE_ORDER);
        buffer.putByte(EncodingConstants.EVENT_TYPE_OFFSET, EncodingConstants.EVENT_TYPE_BOOK_RESET);

        assertThatThrownBy(() -> new SbeRecoveryRequestDecoder().decode(
                buffer,
                0,
                EncodingConstants.MESSAGE_PREFIX_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("templateId");
    }
}
