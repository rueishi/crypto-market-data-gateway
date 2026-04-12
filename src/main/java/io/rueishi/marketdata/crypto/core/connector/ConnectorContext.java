package io.rueishi.marketdata.crypto.core.connector;

import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.util.Map;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

/**
 * Carries bootstrap-owned dependencies into one connector during initialization.
 *
 * <p>{@code ConnectorContext} is created by bootstrap after loading
 * configuration, allocating shared counters, choosing the publisher, and
 * creating connector-local execution resources. A connector receives it in
 * {@code init(ctx)}, unpacks the needed dependencies into its own fields, builds
 * parse/snapshot/recovery contexts, and then discards the wrapper. Connectors
 * must not retain this context after initialization.</p>
 */
public interface ConnectorContext {

    /** @return shared gateway counter facade */
    GatewayCounters counters();

    /** @return shared downstream publisher selected during bootstrap */
    Publisher publisher();

    /** @return connector-local nanosecond clock; concrete type exposes advance() for ClockAdvanceHandler */
    NanoClock nanoClock();

    /** @return epoch clock used for control-path timestamps */
    EpochClock epochClock();

    /** @return transport configuration for this connector */
    TransportConfig transportConfig();

    /** @return dedicated event loop group for this connector */
    EventLoopGroup eventLoopGroup();

    /** @return raw venue-specific configuration block for this process */
    Map<String, Object> venueConfig();
}
