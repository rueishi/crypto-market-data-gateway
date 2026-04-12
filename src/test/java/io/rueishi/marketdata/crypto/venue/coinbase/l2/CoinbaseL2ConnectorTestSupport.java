package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.connector.DefaultConnectorContext;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

/**
 * Shared fixtures for Coinbase L2 connector wiring tests.
 *
 * <p>The helper creates in-memory counters and connector contexts that use
 * injectable clocks, publishers, and event loop groups. Tests use it to avoid
 * touching external Coinbase infrastructure while still exercising production
 * connector and factory code.</p>
 */
final class CoinbaseL2ConnectorTestSupport {
    static final long DEFAULT_NANO_TIME = 9_876_543_210L;
    private static final NanoClock NANO_CLOCK = () -> DEFAULT_NANO_TIME;
    private static final EpochClock EPOCH_CLOCK = () -> 1_700_000_000_000L;

    private CoinbaseL2ConnectorTestSupport() {
    }

    /**
     * Creates gateway counters for one connector test.
     *
     * @return in-memory gateway counters owned by the caller
     */
    static GatewayCounters gatewayCounters() {
        return new GatewayCounters(CoreTestFixtures.newCountersManager(), "gateway-1", "test", "COINBASE_L2");
    }

    /**
     * Creates a connector context with deterministic clocks and supplied runtime dependencies.
     *
     * @param counters gateway counters used by the connector
     * @param publisher in-memory publisher used by the connector
     * @param eventLoopGroup connector event loop group
     * @param config gateway config carrying transport and raw venue config
     * @return connector context ready for {@code Connector.init(ctx)}
     */
    static DefaultConnectorContext context(
            GatewayCounters counters,
            InMemoryPublisher publisher,
            EventLoopGroup eventLoopGroup,
            GatewayConfig config) {
        return context(counters, publisher, eventLoopGroup, config, NANO_CLOCK);
    }

    /**
     * Creates a connector context with a caller-supplied nanosecond clock.
     *
     * @param counters gateway counters used by the connector
     * @param publisher in-memory publisher used by the connector
     * @param eventLoopGroup connector event loop group
     * @param config gateway config carrying transport and raw venue config
     * @param nanoClock connector-local nanosecond clock
     * @return connector context ready for {@code Connector.init(ctx)}
     */
    static DefaultConnectorContext context(
            GatewayCounters counters,
            InMemoryPublisher publisher,
            EventLoopGroup eventLoopGroup,
            GatewayConfig config,
            NanoClock nanoClock) {
        return new DefaultConnectorContext(
                counters,
                publisher,
                nanoClock,
                EPOCH_CLOCK,
                config.transport,
                eventLoopGroup,
                config.venueConfig);
    }
}
