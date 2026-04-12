package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for venue connector factories that parse typed venue configuration.
 *
 * <p>Concrete venue factories extend this class, implement
 * {@link ConnectorFactory#venue()}, parse their raw venue-specific configuration
 * block, and then wire parser, strategy, subscription, authentication, and
 * connector objects. The base class centralizes the common startup sequence so
 * factory implementations do not repeat null checks or raw-map extraction.</p>
 *
 * <p>The factory is used only at startup. It does not receive or retain
 * connector runtime dependencies; those arrive later through
 * {@link Connector#init(ConnectorContext)}.</p>
 */
public abstract class AbstractConnectorFactory implements ConnectorFactory {

    /**
     * Parses venue config and creates one connector for a configured instrument.
     *
     * @param instrument configured instrument
     * @param config validated top-level gateway configuration
     * @return connector wired with venue-specific dependencies
     * @throws NullPointerException if {@code instrument}, {@code config}, or {@code config.venueConfig} is null
     */
    @Override
    public final Connector create(InstrumentConfig instrument, GatewayConfig config) {
        Objects.requireNonNull(instrument, "instrument");
        Objects.requireNonNull(config, "config");
        Map<String, Object> rawConfig = Objects.requireNonNull(config.venueConfig, "config.venueConfig");
        Object venueConfig = parseVenueConfig(rawConfig);
        return Objects.requireNonNull(
                createConnector(instrument, config, venueConfig),
                "createConnector");
    }

    /**
     * Parses the raw venue-specific YAML block into a typed venue configuration object.
     *
     * @param rawConfig raw venue-specific configuration map extracted by {@code ConfigLoader}
     * @return typed config object used by {@link #createConnector(InstrumentConfig, GatewayConfig, Object)}
     */
    protected abstract Object parseVenueConfig(Map<String, Object> rawConfig);

    /**
     * Wires the concrete connector and its venue-specific collaborators.
     *
     * @param instrument configured instrument
     * @param config validated top-level gateway configuration
     * @param venueConfig typed venue-specific configuration from {@link #parseVenueConfig(Map)}
     * @return connector ready for initialization
     */
    protected abstract Connector createConnector(
            InstrumentConfig instrument,
            GatewayConfig config,
            Object venueConfig);
}
