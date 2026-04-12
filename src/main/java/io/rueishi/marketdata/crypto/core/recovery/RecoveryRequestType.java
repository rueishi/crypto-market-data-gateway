package io.rueishi.marketdata.crypto.core.recovery;

/**
 * Type of recovery action requested by downstream state or gateway health checks.
 *
 * <p>{@link RecoveryRequest} carries this value into connector recovery flow.
 * Phase 1 reconnect-style strategies treat all request types the same, but the
 * distinction is preserved so later venue implementations can perform lighter
 * snapshot or resynchronization paths when the exchange supports them.</p>
 */
public enum RecoveryRequestType {
    /** Downstream cannot recover local book state and needs a full reset plus fresh snapshot. */
    RESET,
    /** Downstream wants a fresh checkpoint snapshot, preferably without interrupting a healthy feed. */
    RESNAPSHOT,
    /** Downstream detected an inconsistency and asks the gateway to validate or recover stream state. */
    RESYNC
}
