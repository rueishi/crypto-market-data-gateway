package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SbeRecoveryRequestEncoder}.
 *
 * <p>The tests use in-memory Agrona buffers to verify the downstream-to-gateway
 * recovery control-message encoder independently from gateway routing. Covered
 * scenarios include single-request encoding, batch-request encoding, layout
 * separation from market-data templates, and encoder input validation.</p>
 */
class SbeRecoveryRequestEncoderTest {

    /**
     * Verifies a single `RECOVERY_REQUEST` writes the control template and all request fields.
     */
    @Test
    void encodesSingleRecoveryRequestControlMessage() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        RecoveryRequest request = new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123_456L,
                "manual");

        int length = new SbeRecoveryRequestEncoder().encodeRequest(buffer, 4, request);

        assertThat(length).isEqualTo(EncodingConstants.HEADER_LENGTH
                + SbeRecoveryRequestEncoder.SINGLE_BLOCK_LENGTH
                + "manual".length());
        assertThat(Short.toUnsignedInt(buffer.getShort(4 + EncodingConstants.MAGIC_OFFSET, EncodingConstants.BYTE_ORDER)))
                .isEqualTo(EncodingConstants.MAGIC);
        assertThat(Byte.toUnsignedInt(buffer.getByte(4 + EncodingConstants.TEMPLATE_ID_OFFSET)))
                .isEqualTo(SbeRecoveryRequestEncoder.TEMPLATE_ID_RECOVERY_REQUEST);
        assertThat(Short.toUnsignedInt(buffer.getShort(4 + EncodingConstants.BLOCK_LENGTH_OFFSET, EncodingConstants.BYTE_ORDER)))
                .isEqualTo(SbeRecoveryRequestEncoder.SINGLE_BLOCK_LENGTH);
        assertThat(Short.toUnsignedInt(buffer.getShort(4 + EncodingConstants.ENTRY_COUNT_OFFSET, EncodingConstants.BYTE_ORDER)))
                .isZero();
        int bodyOffset = 4 + EncodingConstants.HEADER_LENGTH;
        assertThat(Byte.toUnsignedInt(buffer.getByte(bodyOffset + SbeRecoveryRequestEncoder.VENUE_RELATIVE_OFFSET)))
                .isEqualTo(VenueEnum.COINBASE_L2.byteValue());
        assertThat(buffer.getInt(
                bodyOffset + SbeRecoveryRequestEncoder.SINGLE_INSTRUMENT_ID_RELATIVE_OFFSET,
                EncodingConstants.BYTE_ORDER)).isEqualTo(1001);
        assertThat(buffer.getLong(
                bodyOffset + SbeRecoveryRequestEncoder.SINGLE_TIMESTAMP_RELATIVE_OFFSET,
                EncodingConstants.BYTE_ORDER)).isEqualTo(123_456L);
    }

    /**
     * Verifies batch encoding writes the common fields once and instrument ids as entries.
     */
    @Test
    void encodesBatchRecoveryRequestControlMessage() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        int[] instrumentIds = {1001, 1002, 1003};

        int length = new SbeRecoveryRequestEncoder().encodeBatch(
                buffer,
                0,
                VenueEnum.COINBASE_L2,
                RecoveryRequestType.RESYNC,
                RecoveryReasonCode.CHECK_FAILED,
                987L,
                instrumentIds);

        assertThat(length).isEqualTo(EncodingConstants.HEADER_LENGTH
                + SbeRecoveryRequestEncoder.BATCH_BLOCK_LENGTH
                + instrumentIds.length * SbeRecoveryRequestEncoder.BATCH_INSTRUMENT_ENTRY_LENGTH);
        assertThat(Byte.toUnsignedInt(buffer.getByte(EncodingConstants.TEMPLATE_ID_OFFSET)))
                .isEqualTo(SbeRecoveryRequestEncoder.TEMPLATE_ID_RECOVERY_REQUEST_BATCH);
        assertThat(Short.toUnsignedInt(buffer.getShort(EncodingConstants.ENTRY_COUNT_OFFSET, EncodingConstants.BYTE_ORDER)))
                .isEqualTo(3);
        int entriesOffset = EncodingConstants.HEADER_LENGTH + SbeRecoveryRequestEncoder.BATCH_BLOCK_LENGTH;
        assertThat(buffer.getInt(entriesOffset, EncodingConstants.BYTE_ORDER)).isEqualTo(1001);
        assertThat(buffer.getInt(entriesOffset + Integer.BYTES, EncodingConstants.BYTE_ORDER)).isEqualTo(1002);
        assertThat(buffer.getInt(entriesOffset + 2 * Integer.BYTES, EncodingConstants.BYTE_ORDER)).isEqualTo(1003);
    }

    /**
     * Verifies invalid control-message inputs fail before partially encoding ambiguous requests.
     */
    @Test
    void rejectsInvalidInputs() {
        SbeRecoveryRequestEncoder encoder = new SbeRecoveryRequestEncoder();
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
        RecoveryRequest request = new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L);

        assertThatThrownBy(() -> encoder.encodeRequest(buffer, 0, request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of bounds");
        assertThatThrownBy(() -> encoder.encodeBatch(
                new UnsafeBuffer(new byte[128]),
                0,
                VenueEnum.COINBASE_L2,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L,
                new int[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrumentIds");
    }
}
