package io.rueishi.marketdata.crypto.core.recovery;

/**
 * Immutable outcome summary for one downstream recovery control message.
 *
 * <p>{@code RecoveryRequestBatchResult} is returned by
 * {@link RecoveryRequestReceiver} after a single SBE recovery-control message is
 * decoded and, when valid, delegated into gateway recovery routing. The result
 * is intentionally small and allocation-friendly because downstream recovery
 * signaling is a control-plane path, not the market-data hot path. Receivers use
 * it to report whether decoded requests were accepted, rejected by runtime
 * state or venue mismatch, rejected because the target instrument is unknown,
 * or rejected before routing because the control message was malformed.</p>
 *
 * @param decodedRequests number of recovery requests decoded from the control message
 * @param acceptedRequests number of decoded requests accepted by gateway routing
 * @param rejectedRequests number of decoded requests rejected for non-instrument reasons
 * @param unknownInstrumentRequests number of decoded requests for instruments not owned by this gateway
 * @param malformedMessages number of malformed control messages rejected before routing
 */
public record RecoveryRequestBatchResult(
        int decodedRequests,
        int acceptedRequests,
        int rejectedRequests,
        int unknownInstrumentRequests,
        int malformedMessages) {

    /**
     * Creates a validated immutable result.
     *
     * @throws IllegalArgumentException if any count is negative
     */
    public RecoveryRequestBatchResult {
        if (decodedRequests < 0
                || acceptedRequests < 0
                || rejectedRequests < 0
                || unknownInstrumentRequests < 0
                || malformedMessages < 0) {
            throw new IllegalArgumentException("result counts must not be negative");
        }
    }

    /**
     * Builds a result for one malformed message that produced no recovery requests.
     *
     * @return result with one malformed message and no decoded requests
     */
    public static RecoveryRequestBatchResult malformed() {
        return new RecoveryRequestBatchResult(0, 0, 0, 0, 1);
    }
}
