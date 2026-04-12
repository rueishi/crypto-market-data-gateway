package io.rueishi.marketdata.crypto.core.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import java.lang.reflect.Field;
import java.util.Map;

import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle tests for the {@link ConnectorContext} ownership pattern.
 *
 * <p>These tests use a tiny connector test double that unpacks context values
 * during initialization. This verifies the intended field-unpacking shape
 * without implementing the later {@code AbstractConnector} lifecycle card.</p>
 */
class ContextLifecycleTest {

    /**
     * Verifies that connector-style code can unpack DefaultConnectorContext without retaining the wrapper.
     */
    @Test
    void connectorInitUnpacksContextWithoutHoldingConnectorContextField() {
        EventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(1);
        try (GatewayCounters counters =
                     new GatewayCounters(CoreTestFixtures.newCountersManager(), "gateway-1", "test", "STUB")) {
            TransportConfig transportConfig = new TransportConfig();

            CachedNanoClock nanoClock = new CachedNanoClock();
            nanoClock.advance(123L);

            EpochClock epochClock = () -> 456L;
            DefaultConnectorContext context = new DefaultConnectorContext(
                    counters,
                    CoreTestFixtures.noopPublisher(),
                    nanoClock,
                    epochClock,
                    transportConfig,
                    eventLoopGroup,
                    Map.of("endpoint", "wss://example.test"));
            ConnectorInitProbe probe = new ConnectorInitProbe();

            probe.init(context);

            assertThat(probe.counters).isSameAs(counters);
            assertThat(probe.transportConfig).isSameAs(transportConfig);
            assertThat(probe.venueConfig).containsEntry("endpoint", "wss://example.test");
            assertThat(ConnectorInitProbe.class.getDeclaredFields())
                    .extracting(Field::getType)
                    .noneMatch(ConnectorContext.class::equals);
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    private static final class ConnectorInitProbe {
        private GatewayCounters counters;
        private TransportConfig transportConfig;
        private Map<String, Object> venueConfig;

        void init(ConnectorContext context) {
            this.counters = context.counters();
            this.transportConfig = context.transportConfig();
            this.venueConfig = context.venueConfig();
        }
    }
}
