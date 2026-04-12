package io.rueishi.marketdata.crypto.core.connector;

import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.util.Map;
import java.util.Objects;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

/**
 * Immutable default implementation of {@link ConnectorContext}.
 *
 * <p>Bootstrap creates one {@code DefaultConnectorContext} per connector after
 * resolving shared and connector-local resources. The class simply carries
 * references across the initialization boundary; connector code should unpack
 * these values inside {@code init(ctx)} and avoid storing the wrapper itself.</p>
 */
public final class DefaultConnectorContext implements ConnectorContext {
    private final GatewayCounters counters;
    private final Publisher publisher;
    private final NanoClock nanoClock;
    private final EpochClock epochClock;
    private final TransportConfig transportConfig;
    private final EventLoopGroup eventLoopGroup;
    private final Map<String, Object> venueConfig;

    /**
     * Creates a connector context from bootstrap-owned dependencies.
     *
     * @param counters shared gateway counters facade
     * @param publisher shared downstream publisher
     * @param nanoClock connector-local cached nanosecond clock; concrete type
     *                  required so {@link io.rueishi.marketdata.crypto.core.transport.ClockAdvanceHandler}
     *                  can call {@link CachedNanoClock#advance()} on each frame
     * @param epochClock control-path epoch clock
     * @param transportConfig transport settings for this connector
     * @param eventLoopGroup dedicated event loop group for this connector
     * @param venueConfig raw venue-specific configuration block
     * @throws NullPointerException if any argument is null
     */
    public DefaultConnectorContext(
            GatewayCounters counters,
            Publisher publisher,
            NanoClock nanoClock,
            EpochClock epochClock,
            TransportConfig transportConfig,
            EventLoopGroup eventLoopGroup,
            Map<String, Object> venueConfig) {
        this.counters = Objects.requireNonNull(counters, "counters");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.epochClock = Objects.requireNonNull(epochClock, "epochClock");
        this.transportConfig = Objects.requireNonNull(transportConfig, "transportConfig");
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        this.venueConfig = Objects.requireNonNull(venueConfig, "venueConfig");
    }

    @Override
    public GatewayCounters counters() {
        return counters;
    }

    @Override
    public Publisher publisher() {
        return publisher;
    }

    @Override
    public NanoClock nanoClock() {
        return nanoClock;
    }

    @Override
    public EpochClock epochClock() {
        return epochClock;
    }

    @Override
    public TransportConfig transportConfig() {
        return transportConfig;
    }

    @Override
    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    @Override
    public Map<String, Object> venueConfig() {
        return venueConfig;
    }
}
