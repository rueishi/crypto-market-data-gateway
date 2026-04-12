package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agrona.MutableDirectBuffer;

/**
 * Encoder for downstream-to-gateway SBE recovery control messages.
 *
 * <p>{@code SbeRecoveryRequestEncoder} writes the control-plane messages used
 * by downstream adapters and tests to request recovery from the gateway. It is
 * intentionally distinct from the market-data {@link io.rueishi.marketdata.crypto.core.encoding.SbeEncoder}:
 * the market-data encoder emits gateway-to-downstream book events such as
 * {@code BOOK_RESET}, while this encoder emits downstream-to-gateway
 * {@code RECOVERY_REQUEST} and {@code RECOVERY_REQUEST_BATCH} messages. The
 * receiver side is {@link SbeRecoveryRequestDecoder} and
 * {@link SbeRecoveryRequestReceiver}.</p>
 *
 * <p>The layout uses the shared SBE-style header fields: magic, version,
 * template id, block length, and entry count. Template ids are deliberately far
 * from the book-event template ids so a market-data {@code BOOK_RESET} is not a
 * valid recovery request.</p>
 */
public final class SbeRecoveryRequestEncoder {
    /** Control template id for a single recovery request message. */
    public static final int TEMPLATE_ID_RECOVERY_REQUEST = 101;
    /** Control template id for a batched recovery request message. */
    public static final int TEMPLATE_ID_RECOVERY_REQUEST_BATCH = 102;

    static final int SINGLE_BLOCK_LENGTH = 18;
    static final int BATCH_BLOCK_LENGTH = 14;
    static final int BATCH_INSTRUMENT_ENTRY_LENGTH = Integer.BYTES;

    static final int VENUE_RELATIVE_OFFSET = 0;
    static final int REQUEST_TYPE_RELATIVE_OFFSET = 1;
    static final int REASON_CODE_RELATIVE_OFFSET = 2;
    static final int RESERVED_RELATIVE_OFFSET = 3;
    static final int SINGLE_INSTRUMENT_ID_RELATIVE_OFFSET = 4;
    static final int SINGLE_TIMESTAMP_RELATIVE_OFFSET = 8;
    static final int SINGLE_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET = 16;
    static final int BATCH_TIMESTAMP_RELATIVE_OFFSET = 4;
    static final int BATCH_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET = 12;

    private static final int MAX_DIAGNOSTIC_BYTES = 65_535;

    /**
     * Encodes one recovery request control message.
     *
     * <p>The optional diagnostic text is encoded as UTF-8 bytes after the fixed
     * block. The method writes {@code entryCount = 0} because a single request
     * carries its instrument id in the fixed body, not a repeating group.</p>
     *
     * @param buffer destination buffer owned by the caller
     * @param offset first byte where the message should be written
     * @param request recovery request to encode
     * @return encoded message length in bytes
     * @throws NullPointerException if {@code buffer} or {@code request} is null
     * @throws IllegalArgumentException if the buffer region is invalid or diagnostic text is too long
     */
    public int encodeRequest(MutableDirectBuffer buffer, int offset, RecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        byte[] diagnosticBytes = diagnosticBytes(request.diagnosticText);
        int length = EncodingConstants.HEADER_LENGTH + SINGLE_BLOCK_LENGTH + diagnosticBytes.length;
        validateWritable(buffer, offset, length);

        writeHeader(buffer, offset, TEMPLATE_ID_RECOVERY_REQUEST, SINGLE_BLOCK_LENGTH, 0);
        int bodyOffset = offset + EncodingConstants.HEADER_LENGTH;
        writeCommonFields(buffer, bodyOffset, request.venue, request.requestType, request.reasonCode);
        buffer.putInt(bodyOffset + SINGLE_INSTRUMENT_ID_RELATIVE_OFFSET, request.instrumentId, EncodingConstants.BYTE_ORDER);
        buffer.putLong(bodyOffset + SINGLE_TIMESTAMP_RELATIVE_OFFSET, request.requestTimestamp, EncodingConstants.BYTE_ORDER);
        writeDiagnostic(buffer, bodyOffset + SINGLE_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET, diagnosticBytes);
        return length;
    }

    /**
     * Encodes a batched recovery request control message.
     *
     * <p>Common request fields are written once, followed by optional UTF-8
     * diagnostic bytes and then a repeating group of instrument ids. The header
     * {@code entryCount} is the number of instrument ids in the batch.</p>
     *
     * @param buffer destination buffer owned by the caller
     * @param offset first byte where the message should be written
     * @param venue target gateway venue
     * @param requestType requested recovery action
     * @param reasonCode informational recovery reason
     * @param requestTimestamp epoch-nanosecond timestamp for the request
     * @param instrumentIds instrument ids to recover
     * @return encoded message length in bytes
     * @throws NullPointerException if a required argument is null
     * @throws IllegalArgumentException if the batch is empty, too large, or cannot fit in the destination buffer
     */
    public int encodeBatch(
            MutableDirectBuffer buffer,
            int offset,
            VenueEnum venue,
            RecoveryRequestType requestType,
            RecoveryReasonCode reasonCode,
            long requestTimestamp,
            int[] instrumentIds) {
        Objects.requireNonNull(instrumentIds, "instrumentIds");
        if (instrumentIds.length == 0) {
            throw new IllegalArgumentException("instrumentIds must not be empty");
        }
        if (instrumentIds.length > EncodingConstants.MAX_SUPPORTED_ENTRIES) {
            throw new IllegalArgumentException(
                    "instrumentIds length exceeds " + EncodingConstants.MAX_SUPPORTED_ENTRIES);
        }
        int length = EncodingConstants.HEADER_LENGTH
                + BATCH_BLOCK_LENGTH
                + Math.multiplyExact(instrumentIds.length, BATCH_INSTRUMENT_ENTRY_LENGTH);
        validateWritable(buffer, offset, length);

        writeHeader(buffer, offset, TEMPLATE_ID_RECOVERY_REQUEST_BATCH, BATCH_BLOCK_LENGTH, instrumentIds.length);
        int bodyOffset = offset + EncodingConstants.HEADER_LENGTH;
        writeCommonFields(buffer, bodyOffset, venue, requestType, reasonCode);
        buffer.putLong(bodyOffset + BATCH_TIMESTAMP_RELATIVE_OFFSET, requestTimestamp, EncodingConstants.BYTE_ORDER);
        writeDiagnostic(buffer, bodyOffset + BATCH_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET, new byte[0]);
        int entryOffset = bodyOffset + BATCH_BLOCK_LENGTH;
        for (int i = 0; i < instrumentIds.length; i++) {
            buffer.putInt(entryOffset + i * BATCH_INSTRUMENT_ENTRY_LENGTH, instrumentIds[i], EncodingConstants.BYTE_ORDER);
        }
        return length;
    }

    /**
     * Writes the shared SBE-style control header used by both recovery templates.
     *
     * @param buffer destination buffer
     * @param offset first byte of the header
     * @param templateId recovery control template id
     * @param blockLength fixed block length for the selected template
     * @param entryCount number of repeating entries, or zero for single requests
     */
    private static void writeHeader(MutableDirectBuffer buffer, int offset, int templateId, int blockLength, int entryCount) {
        buffer.putShort(EncodingConstants.MAGIC_OFFSET + offset, (short) EncodingConstants.MAGIC, EncodingConstants.BYTE_ORDER);
        buffer.putByte(EncodingConstants.VERSION_OFFSET + offset, (byte) EncodingConstants.VERSION);
        buffer.putByte(EncodingConstants.TEMPLATE_ID_OFFSET + offset, (byte) templateId);
        buffer.putShort(EncodingConstants.BLOCK_LENGTH_OFFSET + offset, (short) blockLength, EncodingConstants.BYTE_ORDER);
        buffer.putShort(EncodingConstants.ENTRY_COUNT_OFFSET + offset, (short) entryCount, EncodingConstants.BYTE_ORDER);
    }

    /**
     * Writes the common venue, action, and reason fields shared by both templates.
     *
     * @param buffer destination buffer
     * @param bodyOffset first byte of the fixed body block
     * @param venue target gateway venue
     * @param requestType requested recovery action
     * @param reasonCode informational recovery reason
     * @throws NullPointerException if any common field is null
     */
    private static void writeCommonFields(
            MutableDirectBuffer buffer,
            int bodyOffset,
            VenueEnum venue,
            RecoveryRequestType requestType,
            RecoveryReasonCode reasonCode) {
        buffer.putByte(bodyOffset + VENUE_RELATIVE_OFFSET, Objects.requireNonNull(venue, "venue").byteValue());
        buffer.putByte(bodyOffset + REQUEST_TYPE_RELATIVE_OFFSET, requestTypeByte(requestType));
        buffer.putByte(bodyOffset + REASON_CODE_RELATIVE_OFFSET, reasonCodeByte(reasonCode));
        buffer.putByte(bodyOffset + RESERVED_RELATIVE_OFFSET, (byte) 0);
    }

    /**
     * Writes the variable diagnostic payload length and optional UTF-8 bytes.
     *
     * @param buffer destination buffer
     * @param diagnosticLengthOffset offset of the two-byte diagnostic length field
     * @param diagnosticBytes already validated diagnostic payload bytes
     */
    private static void writeDiagnostic(MutableDirectBuffer buffer, int diagnosticLengthOffset, byte[] diagnosticBytes) {
        buffer.putShort(diagnosticLengthOffset, (short) diagnosticBytes.length, EncodingConstants.BYTE_ORDER);
        if (diagnosticBytes.length > 0) {
            buffer.putBytes(diagnosticLengthOffset + Short.BYTES, diagnosticBytes);
        }
    }

    /**
     * Converts optional diagnostic text to the bounded UTF-8 payload used on the wire.
     *
     * @param diagnosticText optional human-readable recovery context
     * @return UTF-8 encoded bytes, or an empty array when absent
     * @throws IllegalArgumentException if the encoded diagnostic exceeds the unsigned 16-bit length field
     */
    private static byte[] diagnosticBytes(String diagnosticText) {
        if (diagnosticText == null || diagnosticText.isEmpty()) {
            return new byte[0];
        }
        byte[] bytes = diagnosticText.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_DIAGNOSTIC_BYTES) {
            throw new IllegalArgumentException("diagnosticText exceeds " + MAX_DIAGNOSTIC_BYTES + " bytes");
        }
        return bytes;
    }

    /**
     * Maps the internal request type enum to the stable control-message byte code.
     *
     * @param requestType recovery action to encode
     * @return one-byte wire code for the action
     * @throws NullPointerException if {@code requestType} is null
     */
    static byte requestTypeByte(RecoveryRequestType requestType) {
        return switch (Objects.requireNonNull(requestType, "requestType")) {
            case RESET -> 1;
            case RESNAPSHOT -> 2;
            case RESYNC -> 3;
        };
    }

    /**
     * Maps the internal recovery reason enum to the stable control-message byte code.
     *
     * @param reasonCode recovery reason to encode
     * @return one-byte wire code for the reason
     * @throws NullPointerException if {@code reasonCode} is null
     */
    static byte reasonCodeByte(RecoveryReasonCode reasonCode) {
        return switch (Objects.requireNonNull(reasonCode, "reasonCode")) {
            case STREAM_INTEGRITY_FAILURE -> 1;
            case OUT_OF_ORDER_OR_INVALID_TRANSITION -> 2;
            case STATE_CORRUPTION -> 3;
            case CHECK_FAILED -> 4;
            case HEARTBEAT_TIMEOUT -> 5;
            case MANUAL_RESET -> 6;
        };
    }

    /**
     * Validates that the caller-supplied destination region can contain the full message.
     *
     * @param buffer destination buffer
     * @param offset first byte to write
     * @param length encoded message length
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if the region is outside the destination buffer
     */
    private static void validateWritable(MutableDirectBuffer buffer, int offset, int length) {
        Objects.requireNonNull(buffer, "buffer");
        if (offset < 0 || length <= 0 || offset > buffer.capacity() - length) {
            throw new IllegalArgumentException(
                    "encoded recovery request region out of bounds: offset=" + offset
                            + ", length=" + length
                            + ", capacity=" + buffer.capacity());
        }
    }
}
