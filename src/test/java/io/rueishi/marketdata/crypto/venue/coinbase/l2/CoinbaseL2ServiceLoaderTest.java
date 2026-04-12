package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/**
 * ServiceLoader tests for Coinbase L2 factory registration.
 *
 * <p>The suite verifies the production
 * {@code META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory}
 * entry by asking Java {@link ServiceLoader} to discover connector factories
 * from the test classpath. It does not instantiate connectors or open
 * transports.</p>
 */
class CoinbaseL2ServiceLoaderTest {

    /**
     * Verifies ServiceLoader discovers exactly one production Coinbase L2 connector factory.
     */
    @Test
    void serviceLoaderDiscoversCoinbaseL2ConnectorFactory() {
        assertThat(ServiceLoader.load(ConnectorFactory.class))
                .filteredOn(factory -> factory.venue() == VenueEnum.COINBASE_L2)
                .hasOnlyElementsOfType(CoinbaseL2ConnectorFactory.class)
                .hasSize(1);
    }
}
