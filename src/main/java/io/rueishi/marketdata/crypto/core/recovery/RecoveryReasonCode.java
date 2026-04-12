package io.rueishi.marketdata.crypto.core.recovery;

/**
 * Informational reason for a recovery request or internally triggered recovery.
 *
 * <p>The connector and strategy receive this value through
 * {@link RecoveryRequest}. The reason is useful for logs, metrics, and tests,
 * while {@link RecoveryRequestType} controls which recovery behavior should be
 * attempted. Parser and liveness code use these values when autonomously
 * requesting recovery from invalid subscription acknowledgements, invalid feed
 * transitions, and heartbeat timeouts.</p>
 */
public enum RecoveryReasonCode {
    /** Parser, control-plane acknowledgement, sequence tracker, or snapshot alignment detected a stream gap. */
    STREAM_INTEGRITY_FAILURE,
    /** A message arrived out of sequence or would create an invalid book transition. */
    OUT_OF_ORDER_OR_INVALID_TRANSITION,
    /** Gateway-owned book or session state is believed to be corrupted. */
    STATE_CORRUPTION,
    /** A venue or downstream checksum comparison failed. */
    CHECK_FAILED,
    /** Heartbeat or liveness checks exceeded the configured timeout. */
    HEARTBEAT_TIMEOUT,
    /** Operator or downstream control plane requested recovery manually. */
    MANUAL_RESET
}
