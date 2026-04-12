package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.AbstractConnectorFactory;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.util.Map;
import java.util.Objects;
import org.agrona.concurrent.SystemEpochClock;

/**
 * ServiceLoader factory for Coinbase L2 connectors.
 *
 * <p>{@code CoinbaseL2ConnectorFactory} is the startup assembly point for the
 * {@link VenueEnum#COINBASE_L2} venue package. The shared
 * {@link AbstractConnectorFactory} supplies the raw venue config map; this
 * factory parses it into {@link CoinbaseConfig}, creates the Coinbase
 * authenticator, parser, snapshot strategy, Phase 3 reconnect recovery strategy, and
 * subscription builder, then returns one {@link CoinbaseL2Connector} for the
 * supplied instrument.</p>
 *
 * <p>The factory is discovered through
 * {@code META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory}.
 * It runs only during bootstrap and does not receive or retain connector
 * runtime dependencies such as publishers, counters, clocks, or event loops;
 * those arrive later through connector initialization.</p>
 */
public final class CoinbaseL2ConnectorFactory extends AbstractConnectorFactory {

    /**
     * Declares the venue served by this factory.
     *
     * @return {@link VenueEnum#COINBASE_L2}
     */
    @Override
    public VenueEnum venue() {
        return VenueEnum.COINBASE_L2;
    }

    /**
     * Parses raw Coinbase YAML configuration into a typed immutable config object.
     *
     * @param rawConfig raw {@code coinbase:} venue config block
     * @return validated typed Coinbase configuration
     * @throws NullPointerException if {@code rawConfig} is null
     * @throws IllegalArgumentException if required Coinbase config is missing or malformed
     */
    @Override
    protected CoinbaseConfig parseVenueConfig(Map<String, Object> rawConfig) {
        return CoinbaseConfig.from(rawConfig);
    }

    /**
     * Wires one Coinbase L2 connector and its venue-specific collaborators.
     *
     * @param instrument configured instrument served by the connector
     * @param config validated gateway configuration containing encoder sizing
     * @param venueConfig typed Coinbase config returned by {@link #parseVenueConfig(Map)}
     * @return connector ready for initialization by bootstrap
     * @throws NullPointerException if required inputs or encoding config are null
     * @throws ClassCastException if {@code venueConfig} is not a {@link CoinbaseConfig}
     */
    @Override
    protected Connector createConnector(InstrumentConfig instrument, GatewayConfig config, Object venueConfig) {
        CoinbaseConfig coinbaseConfig = (CoinbaseConfig) Objects.requireNonNull(venueConfig, "venueConfig");
        EncodingConfig encoding = Objects.requireNonNull(config.encoding, "config.encoding");
        Objects.requireNonNull(config.transport, "config.transport");
        CoinbaseAuthenticator authenticator = new CoinbaseAuthenticator(coinbaseConfig);
        CoinbaseL2SubscriptionBuilder subscription =
                new CoinbaseL2SubscriptionBuilder(coinbaseConfig, authenticator, SystemEpochClock.INSTANCE);
        return new CoinbaseL2Connector(
                instrument,
                new CoinbaseL2FeedParser(instrument),
                new CoinbaseL2SnapshotStrategy(),
                subscription,
                coinbaseConfig,
                config.transport,
                encoding.maxLevelsPerMessage,
                encoding.bufferHeadroomBytes);
    }
}
