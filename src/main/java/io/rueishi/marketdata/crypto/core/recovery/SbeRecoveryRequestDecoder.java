package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agrona.DirectBuffer;

/**
 * Decoder for downstream-to-gateway SBE recovery control messages.
 *
 * <p>{@code SbeRecoveryRequestDecoder} validates the control-message header and
 * payload before constructing {@link RecoveryRequest} objects. It is used by
 * {@link SbeRecoveryRequestReceiver} at the gateway boundary and pairs with
 * {@link SbeRecoveryRequestEncoder}. It deliberately rejects market-data
 * templates, including {@code BOOK_RESET}, because downstream must request
 * recovery through {@code RECOVERY_REQUEST} or {@code RECOVERY_REQUEST_BATCH}
 * rather than sending gateway-to-downstream book events back to the gateway.</p>
 */
public final class SbeRecoveryRequestDecoder {

    /**
     * Decodes one control message into one or more recovery requests.
     *
     * <p>The method reads all fields from the caller-owned buffer immediately
     * and returns newly allocated request metadata. Invalid magic, version,
     * template id, block length, enum values, payload length, or empty batch
     * inputs fail with {@link IllegalArgumentException} before any routing can
     * occur.</p>
     *
     * @param buffer buffer containing the control message
     * @param offset first byte of the message inside {@code buffer}
     * @param length number of bytes available for the message
     * @return decoded recovery requests
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if the message is malformed or unsupported
     */
    public List<RecoveryRequest> decode(DirectBuffer buffer, int offset, int length) {
        validateReadable(buffer, offset, length);
        int magic = Short.toUnsignedInt(buffer.getShort(offset + EncodingConstants.MAGIC_OFFSET, EncodingConstants.BYTE_ORDER));
        if (magic != EncodingConstants.MAGIC) {
            throw new IllegalArgumentException("Invalid recovery request magic: " + magic);
        }
        int version = Byte.toUnsignedInt(buffer.getByte(offset + EncodingConstants.VERSION_OFFSET));
        if (version != EncodingConstants.VERSION) {
            throw new IllegalArgumentException("Unsupported recovery request version: " + version);
        }
        int templateId = Byte.toUnsignedInt(buffer.getByte(offset + EncodingConstants.TEMPLATE_ID_OFFSET));
        int blockLength = Short.toUnsignedInt(buffer.getShort(
                offset + EncodingConstants.BLOCK_LENGTH_OFFSET,
                EncodingConstants.BYTE_ORDER));
        int entryCount = Short.toUnsignedInt(buffer.getShort(
                offset + EncodingConstants.ENTRY_COUNT_OFFSET,
                EncodingConstants.BYTE_ORDER));
        return switch (templateId) {
            case SbeRecoveryRequestEncoder.TEMPLATE_ID_RECOVERY_REQUEST ->
                    decodeSingle(buffer, offset, length, blockLength, entryCount);
            case SbeRecoveryRequestEncoder.TEMPLATE_ID_RECOVERY_REQUEST_BATCH ->
                    decodeBatch(buffer, offset, length, blockLength, entryCount);
            default -> throw new IllegalArgumentException("Unsupported recovery request templateId: " + templateId);
        };
    }

    /**
     * Decodes the single-instrument template after the shared header has been validated.
     *
     * @param buffer source buffer containing the full control message
     * @param offset first byte of the message
     * @param length supplied message length
     * @param blockLength fixed block length from the header
     * @param entryCount repeating entry count from the header, expected to be zero
     * @return immutable singleton list containing the decoded recovery request
     * @throws IllegalArgumentException if the block length, entry count, or total length is invalid
     */
    private static List<RecoveryRequest> decodeSingle(
            DirectBuffer buffer,
            int offset,
            int length,
            int blockLength,
            int entryCount) {
        if (blockLength != SbeRecoveryRequestEncoder.SINGLE_BLOCK_LENGTH) {
            throw new IllegalArgumentException("Invalid single recovery request blockLength: " + blockLength);
        }
        if (entryCount != 0) {
            throw new IllegalArgumentException("Single recovery request entryCount must be 0");
        }
        int bodyOffset = offset + EncodingConstants.HEADER_LENGTH;
        int diagnosticLength = diagnosticLength(buffer, bodyOffset + SbeRecoveryRequestEncoder.SINGLE_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET);
        int expectedLength = EncodingConstants.HEADER_LENGTH + blockLength + diagnosticLength;
        requireExactLength(length, expectedLength);
        RecoveryRequest request = new RecoveryRequest(
                decodeVenue(buffer, bodyOffset),
                buffer.getInt(
                        bodyOffset + SbeRecoveryRequestEncoder.SINGLE_INSTRUMENT_ID_RELATIVE_OFFSET,
                        EncodingConstants.BYTE_ORDER),
                decodeRequestType(buffer, bodyOffset),
                decodeReasonCode(buffer, bodyOffset),
                buffer.getLong(
                        bodyOffset + SbeRecoveryRequestEncoder.SINGLE_TIMESTAMP_RELATIVE_OFFSET,
                        EncodingConstants.BYTE_ORDER),
                diagnosticText(buffer, bodyOffset + blockLength, diagnosticLength));
        return List.of(request);
    }

    /**
     * Decodes the multi-instrument template into independent recovery requests.
     *
     * <p>The batch carries shared venue, recovery action, reason, timestamp, and
     * optional diagnostic text once, followed by the instrument id repeating
     * group. The receiver routes each returned request independently.</p>
     *
     * @param buffer source buffer containing the full control message
     * @param offset first byte of the message
     * @param length supplied message length
     * @param blockLength fixed block length from the header
     * @param entryCount number of instrument id entries in the repeating group
     * @return immutable list of decoded recovery requests
     * @throws IllegalArgumentException if the batch is empty or its layout is malformed
     */
    private static List<RecoveryRequest> decodeBatch(
            DirectBuffer buffer,
            int offset,
            int length,
            int blockLength,
            int entryCount) {
        if (blockLength != SbeRecoveryRequestEncoder.BATCH_BLOCK_LENGTH) {
            throw new IllegalArgumentException("Invalid batch recovery request blockLength: " + blockLength);
        }
        if (entryCount == 0) {
            throw new IllegalArgumentException("Batch recovery request must contain at least one instrument");
        }
        int bodyOffset = offset + EncodingConstants.HEADER_LENGTH;
        int diagnosticLength = diagnosticLength(buffer, bodyOffset + SbeRecoveryRequestEncoder.BATCH_DIAGNOSTIC_LENGTH_RELATIVE_OFFSET);
        int entriesOffset = bodyOffset + blockLength + diagnosticLength;
        int expectedLength = EncodingConstants.HEADER_LENGTH
                + blockLength
                + diagnosticLength
                + Math.multiplyExact(entryCount, SbeRecoveryRequestEncoder.BATCH_INSTRUMENT_ENTRY_LENGTH);
        requireExactLength(length, expectedLength);

        VenueEnum venue = decodeVenue(buffer, bodyOffset);
        RecoveryRequestType requestType = decodeRequestType(buffer, bodyOffset);
        RecoveryReasonCode reasonCode = decodeReasonCode(buffer, bodyOffset);
        long requestTimestamp = buffer.getLong(
                bodyOffset + SbeRecoveryRequestEncoder.BATCH_TIMESTAMP_RELATIVE_OFFSET,
                EncodingConstants.BYTE_ORDER);
        String diagnosticText = diagnosticText(buffer, bodyOffset + blockLength, diagnosticLength);
        List<RecoveryRequest> requests = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int instrumentId = buffer.getInt(
                    entriesOffset + i * SbeRecoveryRequestEncoder.BATCH_INSTRUMENT_ENTRY_LENGTH,
                    EncodingConstants.BYTE_ORDER);
            requests.add(new RecoveryRequest(
                    venue,
                    instrumentId,
                    requestType,
                    reasonCode,
                    requestTimestamp,
                    diagnosticText));
        }
        return List.copyOf(requests);
    }

    /**
     * Maps the venue wire byte to the gateway venue enum.
     *
     * @param buffer source buffer
     * @param bodyOffset first byte of the fixed body block
     * @return decoded venue enum
     * @throws IllegalArgumentException if the venue byte is unknown
     */
    private static VenueEnum decodeVenue(DirectBuffer buffer, int bodyOffset) {
        int venueByte = Byte.toUnsignedInt(buffer.getByte(bodyOffset + SbeRecoveryRequestEncoder.VENUE_RELATIVE_OFFSET));
        for (VenueEnum venue : VenueEnum.values()) {
            if (Byte.toUnsignedInt(venue.byteValue()) == venueByte) {
                return venue;
            }
        }
        throw new IllegalArgumentException("Unknown recovery request venue byte: " + venueByte);
    }

    /**
     * Maps the request-type wire byte to the internal recovery action enum.
     *
     * @param buffer source buffer
     * @param bodyOffset first byte of the fixed body block
     * @return decoded recovery action
     * @throws IllegalArgumentException if the request-type byte is unknown
     */
    private static RecoveryRequestType decodeRequestType(DirectBuffer buffer, int bodyOffset) {
        int requestTypeByte =
                Byte.toUnsignedInt(buffer.getByte(bodyOffset + SbeRecoveryRequestEncoder.REQUEST_TYPE_RELATIVE_OFFSET));
        return switch (requestTypeByte) {
            case 1 -> RecoveryRequestType.RESET;
            case 2 -> RecoveryRequestType.RESNAPSHOT;
            case 3 -> RecoveryRequestType.RESYNC;
            default -> throw new IllegalArgumentException("Unknown recovery request type byte: " + requestTypeByte);
        };
    }

    /**
     * Maps the reason-code wire byte to the internal recovery reason enum.
     *
     * @param buffer source buffer
     * @param bodyOffset first byte of the fixed body block
     * @return decoded recovery reason
     * @throws IllegalArgumentException if the reason-code byte is unknown
     */
    private static RecoveryReasonCode decodeReasonCode(DirectBuffer buffer, int bodyOffset) {
        int reasonCodeByte =
                Byte.toUnsignedInt(buffer.getByte(bodyOffset + SbeRecoveryRequestEncoder.REASON_CODE_RELATIVE_OFFSET));
        return switch (reasonCodeByte) {
            case 1 -> RecoveryReasonCode.STREAM_INTEGRITY_FAILURE;
            case 2 -> RecoveryReasonCode.OUT_OF_ORDER_OR_INVALID_TRANSITION;
            case 3 -> RecoveryReasonCode.STATE_CORRUPTION;
            case 4 -> RecoveryReasonCode.CHECK_FAILED;
            case 5 -> RecoveryReasonCode.HEARTBEAT_TIMEOUT;
            case 6 -> RecoveryReasonCode.MANUAL_RESET;
            default -> throw new IllegalArgumentException("Unknown recovery reason code byte: " + reasonCodeByte);
        };
    }

    private static int diagnosticLength(DirectBuffer buffer, int offset) {
        return Short.toUnsignedInt(buffer.getShort(offset, EncodingConstants.BYTE_ORDER));
    }

    private static String diagnosticText(DirectBuffer buffer, int offset, int length) {
        if (length == 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Validates that the caller-supplied source region can contain a control header.
     *
     * @param buffer source buffer
     * @param offset first byte to read
     * @param length available message length
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if the region is outside the source buffer or cannot contain a header
     */
    private static void validateReadable(DirectBuffer buffer, int offset, int length) {
        Objects.requireNonNull(buffer, "buffer");
        if (offset < 0 || length < EncodingConstants.HEADER_LENGTH || offset > buffer.capacity() - length) {
            throw new IllegalArgumentException(
                    "recovery request region out of bounds: offset=" + offset
                            + ", length=" + length
                            + ", capacity=" + buffer.capacity());
        }
    }

    /**
     * Enforces exact-length messages so trailing or truncated bytes are rejected before routing.
     *
     * @param actualLength length supplied by the caller
     * @param expectedLength length derived from the decoded header and body fields
     * @throws IllegalArgumentException if the supplied length does not match the decoded layout
     */
    private static void requireExactLength(int actualLength, int expectedLength) {
        if (actualLength != expectedLength) {
            throw new IllegalArgumentException(
                    "Invalid recovery request length: expected " + expectedLength + " but was " + actualLength);
        }
    }
}
