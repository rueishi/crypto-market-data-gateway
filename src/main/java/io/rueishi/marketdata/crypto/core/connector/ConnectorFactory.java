package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;

/**
 * Service-provider interface for venue-specific connector factories.
 *
 * <p>Bootstrap and the later venue registry discover one factory per venue via
 * {@code ServiceLoader}. The factory converts validated startup configuration
 * into a connector instance for each configured instrument. Gateway-wide
 * runtime resources, such as counters, publisher, and connector-local clocks,
 * arrive later through {@link Connector#init(ConnectorContext)}.</p>
 */
public interface ConnectorFactory {

    /**
     * Returns the venue served by this factory.
     *
     * @return venue/depth identity used for registry matching
     */
    VenueEnum venue();

    /**
     * Creates a connector for one configured instrument.
     *
     * <p>This call happens during startup, before hot-path processing begins, so
     * venue factories may parse typed config and allocate venue-specific helper
     * objects here.</p>
     *
     * @param instrument configured instrument for the connector
     * @param config validated top-level gateway configuration
     * @return connector ready to receive {@link Connector#init(ConnectorContext)}
     */
    Connector create(InstrumentConfig instrument, GatewayConfig config);
}
