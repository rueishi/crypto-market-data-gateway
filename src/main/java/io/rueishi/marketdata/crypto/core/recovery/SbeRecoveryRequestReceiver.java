package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.bootstrap.GatewayRuntime;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;

/**
 * Gateway runtime adapter for SBE downstream recovery control messages.
 *
 * <p>{@code SbeRecoveryRequestReceiver} is normally created from a live
 * {@link GatewayRuntime} and exposed to whichever downstream control-plane
 * adapter is responsible for receiving binary recovery requests. It decodes the
 * caller-owned buffer with {@link SbeRecoveryRequestDecoder}, then delegates
 * every decoded {@link RecoveryRequest} through
 * {@link GatewayRuntime#requestRecovery(RecoveryRequest)}. That reuse keeps the
 * receiver focused on boundary validation and preserves the existing
 * {@link RecoveryRequestRouter} routing and connector-owned coalescing
 * behavior.</p>
 */
public final class SbeRecoveryRequestReceiver implements RecoveryRequestReceiver {
    private final Function<RecoveryRequest, Boolean> requestRecovery;
    private final Supplier<VenueEnum> configuredVenue;
    private final Supplier<List<Connector>> connectors;
    private final SbeRecoveryRequestDecoder decoder;

    /**
     * Creates a receiver around the gateway runtime recovery entry point.
     *
     * @param runtime initialized runtime that owns connector recovery routing
     * @throws NullPointerException if {@code runtime} is null
     */
    public SbeRecoveryRequestReceiver(GatewayRuntime runtime) {
        Objects.requireNonNull(runtime, "runtime");
        this.requestRecovery = runtime::requestRecovery;
        this.configuredVenue = () -> runtime.config().venue;
        this.connectors = runtime::connectors;
        this.decoder = new SbeRecoveryRequestDecoder();
    }

    /**
     * Creates a receiver with injectable routing collaborators for focused tests.
     *
     * <p>The production path uses {@link #SbeRecoveryRequestReceiver(GatewayRuntime)}.
     * This constructor keeps decoder and result-classification tests independent
     * from full bootstrap setup while still exercising the same receive logic.</p>
     *
     * @param requestRecovery recovery routing function, usually {@link GatewayRuntime#requestRecovery(RecoveryRequest)}
     * @param configuredVenue supplier for the runtime's configured venue
     * @param connectors supplier for runtime-owned connectors
     * @param decoder decoder used to parse SBE recovery control messages
     * @throws NullPointerException if any argument is null
     */
    SbeRecoveryRequestReceiver(
            Function<RecoveryRequest, Boolean> requestRecovery,
            Supplier<VenueEnum> configuredVenue,
            Supplier<List<Connector>> connectors,
            SbeRecoveryRequestDecoder decoder) {
        this.requestRecovery = Objects.requireNonNull(requestRecovery, "requestRecovery");
        this.configuredVenue = Objects.requireNonNull(configuredVenue, "configuredVenue");
        this.connectors = Objects.requireNonNull(connectors, "connectors");
        this.decoder = Objects.requireNonNull(decoder, "decoder");
    }

    /**
     * Decodes one SBE control message and routes the contained recovery requests.
     *
     * <p>Malformed messages return a malformed result and never invoke
     * {@link GatewayRuntime#requestRecovery(RecoveryRequest)}. Valid messages
     * route each decoded request independently. If the runtime rejects a request,
     * the receiver classifies matching-venue/unknown-instrument requests
     * separately from other rejections such as venue mismatch or shutdown.</p>
     *
     * @param buffer buffer containing a downstream-to-gateway recovery control message
     * @param offset first byte of the message inside {@code buffer}
     * @param length number of bytes available for the message
     * @return decode and routing result for this control message
     */
    @Override
    public RecoveryRequestBatchResult receive(DirectBuffer buffer, int offset, int length) {
        final List<RecoveryRequest> requests;
        try {
            requests = decoder.decode(buffer, offset, length);
        } catch (IllegalArgumentException ex) {
            return RecoveryRequestBatchResult.malformed();
        }

        int accepted = 0;
        int rejected = 0;
        int unknownInstrument = 0;
        for (RecoveryRequest request : requests) {
            if (requestRecovery.apply(request)) {
                accepted++;
            } else if (isUnknownInstrument(request)) {
                unknownInstrument++;
            } else {
                rejected++;
            }
        }
        return new RecoveryRequestBatchResult(requests.size(), accepted, rejected, unknownInstrument, 0);
    }

    /**
     * Determines whether a routed rejection was caused by a missing instrument connector.
     *
     * <p>{@link GatewayRuntime#requestRecovery(RecoveryRequest)} intentionally
     * returns only a boolean, so the receiver uses the runtime's configured
     * venue and connector list to provide a more useful control-plane result.
     * Requests for other venues are classified as general rejections rather
     * than unknown instruments for this gateway.</p>
     *
     * @param request decoded recovery request rejected by runtime routing
     * @return true when the request targets this gateway venue but no connector owns the instrument
     */
    private boolean isUnknownInstrument(RecoveryRequest request) {
        if (request.venue != configuredVenue.get()) {
            return false;
        }
        return connectors.get().stream()
                .noneMatch(connector -> connector.venue() == request.venue
                        && connector.instrumentId() == request.instrumentId);
    }
}
