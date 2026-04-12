package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import java.util.Objects;

/**
 * Immutable metadata for one connector recovery request.
 *
 * <p>A connector receives this object from downstream control flow, internal
 * stream-integrity checks, or future liveness checks. The request identifies
 * the target {@link VenueEnum}, instrument id, requested recovery action,
 * informational reason, request timestamp, and optional diagnostic text. In
 * Phase 1 the request is passed into {@link RecoveryStrategy#execute(RecoveryRequest, RecoveryContext)}
 * after the connector publishes a reset and builds a fresh recovery context.</p>
 *
 * <p>The optional diagnostic text is intended for off-path failure reporting.
 * Hot-path callers may pass {@code null} to avoid allocating strings.</p>
 */
public final class RecoveryRequest {
    /** Target venue instance; connector code later ignores requests for other venues. */
    public final VenueEnum venue;
    /** Stable internal instrument id targeted by the request. */
    public final int instrumentId;
    /** Requested recovery action. */
    public final RecoveryRequestType requestType;
    /** Informational reason for the request. */
    public final RecoveryReasonCode reasonCode;
    /** Epoch-nanosecond timestamp when the request was generated. */
    public final long requestTimestamp;
    /** Optional diagnostic text for logs or operations; may be null on the hot path. */
    public final String diagnosticText;

    /**
     * Creates a recovery request without optional diagnostic text.
     *
     * @param venue target venue instance
     * @param instrumentId stable internal instrument id
     * @param requestType requested recovery action
     * @param reasonCode informational recovery reason
     * @param requestTimestamp epoch-nanosecond timestamp when the request was generated
     * @throws NullPointerException if {@code venue}, {@code requestType}, or {@code reasonCode} is null
     */
    public RecoveryRequest(
            VenueEnum venue,
            int instrumentId,
            RecoveryRequestType requestType,
            RecoveryReasonCode reasonCode,
            long requestTimestamp) {
        this(venue, instrumentId, requestType, reasonCode, requestTimestamp, null);
    }

    /**
     * Creates a recovery request with optional diagnostic text.
     *
     * @param venue target venue instance
     * @param instrumentId stable internal instrument id
     * @param requestType requested recovery action
     * @param reasonCode informational recovery reason
     * @param requestTimestamp epoch-nanosecond timestamp when the request was generated
     * @param diagnosticText optional diagnostic text for logs or operations
     * @throws NullPointerException if {@code venue}, {@code requestType}, or {@code reasonCode} is null
     */
    public RecoveryRequest(
            VenueEnum venue,
            int instrumentId,
            RecoveryRequestType requestType,
            RecoveryReasonCode reasonCode,
            long requestTimestamp,
            String diagnosticText) {
        this.venue = Objects.requireNonNull(venue, "venue");
        this.instrumentId = instrumentId;
        this.requestType = Objects.requireNonNull(requestType, "requestType");
        this.reasonCode = Objects.requireNonNull(reasonCode, "reasonCode");
        this.requestTimestamp = requestTimestamp;
        this.diagnosticText = diagnosticText;
    }
}
