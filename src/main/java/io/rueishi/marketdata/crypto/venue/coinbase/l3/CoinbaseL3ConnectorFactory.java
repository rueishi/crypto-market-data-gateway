package io.rueishi.marketdata.crypto.venue.coinbase.l3;

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
 * ServiceLoader factory for Coinbase L3 full-order feed connectors.
 *
 * <p>{@code CoinbaseL3ConnectorFactory} is the startup assembly point for the
 * {@link VenueEnum#COINBASE_L3} venue package. The shared
 * {@link AbstractConnectorFactory} supplies the raw venue config map; this
 * factory parses it into {@link CoinbaseConfig}, creates the Coinbase
 * authenticator, parser, subscription builder, and recovery strategy, then
 * returns one {@link CoinbaseL3Connector} per supplied instrument.</p>
 *
 * <p>The {@link CoinbaseL3SnapshotStrategy} requires a live
 * {@link io.rueishi.marketdata.crypto.core.parser.ParseContext} and an
 * event-loop executor, neither of which is available at factory time. The
 * factory creates the connector via its deferred-snapshot constructor, which
 * installs the real strategy inside
 * {@link CoinbaseL3Connector#onInitialized(io.rueishi.marketdata.crypto.core.connector.ConnectorContext)}
 * after bootstrap calls
 * {@link io.rueishi.marketdata.crypto.core.connector.Connector#init(io.rueishi.marketdata.crypto.core.connector.ConnectorContext)}.</p>
 *
 * <p>The factory is discovered through
 * {@code META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory}.
 * It runs only during bootstrap and does not receive or retain connector
 * runtime dependencies such as publishers, counters, clocks, or event loops;
 * those arrive later through connector initialization.</p>
 */
public final class CoinbaseL3ConnectorFactory extends AbstractConnectorFactory {

    /**
     * Declares the venue served by this factory.
     *
     * @return {@link VenueEnum#COINBASE_L3}
     */
    @Override
    public VenueEnum venue() {
        return VenueEnum.COINBASE_L3;
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
     * Wires one Coinbase L3 connector and its venue-specific collaborators.
     *
     * <p>The {@link CoinbaseL3SnapshotStrategy} is intentionally deferred: the
     * factory creates the connector with an internal
     * {@link CoinbaseL3Connector.DeferredSnapshotHolder}, and the connector
     * installs the real strategy in
     * {@link CoinbaseL3Connector#onInitialized(io.rueishi.marketdata.crypto.core.connector.ConnectorContext)}
     * once the {@link io.rueishi.marketdata.crypto.core.parser.ParseContext} and
     * event-loop executor are available.</p>
     *
     * @param instrument configured instrument served by the connector
     * @param config validated gateway configuration
     * @param venueConfig typed Coinbase config returned by {@link #parseVenueConfig(Map)}
     * @return connector ready for initialization by bootstrap
     * @throws NullPointerException if required inputs or transport config are null
     * @throws ClassCastException if {@code venueConfig} is not a {@link CoinbaseConfig}
     */
    @Override
    protected Connector createConnector(InstrumentConfig instrument, GatewayConfig config, Object venueConfig) {
        CoinbaseConfig coinbaseConfig = (CoinbaseConfig) Objects.requireNonNull(venueConfig, "venueConfig");
        Objects.requireNonNull(config.transport, "config.transport");
        CoinbaseAuthenticator authenticator = new CoinbaseAuthenticator(coinbaseConfig);
        CoinbaseL3FeedParser parser = new CoinbaseL3FeedParser(instrument);
        CoinbaseL3SubscriptionBuilder subscription =
                new CoinbaseL3SubscriptionBuilder(coinbaseConfig, authenticator, SystemEpochClock.INSTANCE);
        CoinbaseL3Connector.DeferredRecoveryActions recoveryActions =
                new CoinbaseL3Connector.DeferredRecoveryActions();
        CoinbaseL3RecoveryStrategy recovery =
                new CoinbaseL3RecoveryStrategy(recoveryActions, config.transport);
        CoinbaseL3Connector connector =
                new CoinbaseL3Connector(instrument, parser, recovery, subscription, authenticator);
        recoveryActions.setDelegate(connector);
        return connector;
    }
}
