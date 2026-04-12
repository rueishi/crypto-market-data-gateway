package io.rueishi.marketdata.crypto.core.recovery;

import org.agrona.DirectBuffer;

/**
 * Control-plane receiver contract for downstream-triggered recovery requests.
 *
 * <p>{@code RecoveryRequestReceiver} is the gateway-side boundary used by
 * downstream adapters that want to request recovery for one or more instruments.
 * It receives a caller-owned SBE-style control buffer, decodes it into
 * {@link RecoveryRequest} metadata, and delegates accepted requests into the
 * same recovery routing path used by internal parser and liveness triggers. The
 * receiver is deliberately separate from the market-data publisher and
 * transport classes: downstream sends {@code RECOVERY_REQUEST} control messages
 * to this interface, while the gateway remains responsible for publishing
 * {@code BOOK_RESET} back downstream after recovery starts.</p>
 */
public interface RecoveryRequestReceiver {

    /**
     * Decodes and routes one downstream recovery control message.
     *
     * <p>The supplied buffer remains owned by the caller. Implementations must
     * finish reading the region during this call and must not retain the buffer
     * reference. Malformed messages are rejected before recovery routing.</p>
     *
     * @param buffer buffer containing a downstream-to-gateway recovery control message
     * @param offset first byte of the message inside {@code buffer}
     * @param length number of bytes available for the message
     * @return per-message routing and validation result
     * @throws NullPointerException if {@code buffer} is null
     */
    RecoveryRequestBatchResult receive(DirectBuffer buffer, int offset, int length);
}
