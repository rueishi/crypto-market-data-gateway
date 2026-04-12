package io.rueishi.marketdata.crypto.core.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link VenueRegistry}.
 *
 * <p>The tests cover the registry's ServiceLoader discovery path, missing
 * factory failure, and duplicate factory detection. The positive discovery
 * scenario uses the production Coinbase L2 provider registered in main
 * resources; the negative scenarios use small local factories so the global
 * ServiceLoader metadata remains stable for the rest of the test suite.</p>
 */
class VenueRegistryTest {

    /**
     * Verifies that the registry resolves the Coinbase L2 factory through ServiceLoader rather than direct construction.
     */
    @Test
    void resolvesCoinbaseL2ConnectorFactoryThroughServiceLoader() {
        ConnectorFactory factory = new VenueRegistry().forVenue(VenueEnum.COINBASE_L2);

        assertThat(factory.venue()).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(factory.getClass().getName())
                .isEqualTo("io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2ConnectorFactory");
    }

    /**
     * Verifies that missing venue providers fail early with an actionable ServiceLoader hint.
     */
    @Test
    void missingVenueFailsWithServiceLoaderHint() {
        VenueRegistry registry = new VenueRegistry(List.of(new LocalFactory(VenueEnum.COINBASE_L2)));

        assertThatThrownBy(() -> registry.forVenue(VenueEnum.BINANCE_L2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No ConnectorFactory registered for venue: BINANCE_L2")
                .hasMessageContaining("META-INF/services");
    }

    /**
     * Verifies that duplicate providers for one venue fail before bootstrap can create connectors.
     */
    @Test
    void duplicateVenueFactoriesFailDuringRegistryConstruction() {
        assertThatThrownBy(() -> new VenueRegistry(List.of(
                        new LocalFactory(VenueEnum.COINBASE_L2),
                        new LocalFactory(VenueEnum.COINBASE_L2))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate ConnectorFactory for venue: COINBASE_L2");
    }

    private record LocalFactory(VenueEnum venue) implements ConnectorFactory {
        @Override
        public Connector create(InstrumentConfig instrument, GatewayConfig config) {
            throw new UnsupportedOperationException("not used by registry tests");
        }
    }
}
