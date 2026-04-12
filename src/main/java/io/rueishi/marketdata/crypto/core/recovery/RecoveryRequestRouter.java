package io.rueishi.marketdata.crypto.core.recovery;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Routes downstream recovery requests to the connector that owns the target stream.
 *
 * <p>{@code RecoveryRequestRouter} is constructed by bootstrap after all
 * connectors have been initialized. It builds an immutable lookup table keyed
 * by {@link VenueEnum} and instrument id, keeping recovery routing out of
 * {@code GatewayBootstrap} while leaving venue-specific recovery strategy work
 * inside the connector selected by the route.</p>
 *
 * <p>The router is deliberately narrow: it ignores venue mismatches and unknown
 * instruments, and it invokes {@link Connector#requestRecovery(RecoveryRequest)}
 * only for an exact match. Connector implementations, especially
 * {@code AbstractConnector}, remain the authoritative final guard for
 * duplicate in-progress coalescing and request validation.</p>
 */
public final class RecoveryRequestRouter {
    private final Map<RouteKey, Connector> connectorsByRoute;

    /**
     * Builds a router over initialized connector instances.
     *
     * @param connectors connectors retained by the gateway runtime
     * @throws NullPointerException if {@code connectors}, a connector, or connector venue is null
     * @throws IllegalArgumentException if two connectors declare the same venue and instrument id
     */
    public RecoveryRequestRouter(Collection<? extends Connector> connectors) {
        Objects.requireNonNull(connectors, "connectors");
        Map<RouteKey, Connector> routes = new HashMap<>();
        for (Connector connector : connectors) {
            Objects.requireNonNull(connector, "connector");
            RouteKey key = new RouteKey(
                    Objects.requireNonNull(connector.venue(), "connector.venue"),
                    connector.instrumentId());
            Connector previous = routes.put(key, connector);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate connector route for venue " + key.venue()
                                + " and instrumentId " + key.instrumentId());
            }
        }
        this.connectorsByRoute = Map.copyOf(routes);
    }

    /**
     * Routes a recovery request to the owning connector when one exists.
     *
     * <p>The method returns whether a matching connector accepted the request
     * for scheduling. It does not report whether the connector later coalesced
     * the request as a duplicate, because that decision happens inside the
     * connector on its event-loop path.</p>
     *
     * @param request downstream or control-plane recovery request
     * @return {@code true} if an exact connector route was found; {@code false} for mismatched venues or unknown instruments
     * @throws NullPointerException if {@code request} is null
     */
    public boolean route(RecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        Connector connector = connectorsByRoute.get(new RouteKey(request.venue, request.instrumentId));
        if (connector == null) {
            return false;
        }

        connector.requestRecovery(request);
        return true;
    }

    private record RouteKey(VenueEnum venue, int instrumentId) {
    }
}
