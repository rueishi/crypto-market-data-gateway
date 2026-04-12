package io.rueishi.marketdata.crypto.core.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AbstractConnectorFactory}.
 *
 * <p>The test factory parses a raw venue config map into a tiny typed config
 * object and then wires a stub connector. This verifies the base factory's
 * startup-only parse-then-create sequence without depending on any real venue
 * package.</p>
 */
class AbstractConnectorFactoryTest {

    /**
     * Verifies that create parses raw config and passes typed config to connector construction.
     */
    @Test
    void createParsesVenueConfigAndCreatesConnector() {
        TestConnectorFactory factory = new TestConnectorFactory();
        GatewayConfig config = new GatewayConfig();
        config.venue = VenueEnum.COINBASE_L2;
        config.venueConfig = Map.of("endpoint", "wss://example.test");
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = "BTC-USD";
        instrument.instrumentId = 1001;

        Connector connector = factory.create(instrument, config);

        assertThat(factory.parsedConfig.endpoint).isEqualTo("wss://example.test");
        assertThat(factory.createSawParsedConfig).isTrue();
        assertThat(connector.venue()).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(connector.instrumentId()).isEqualTo(1001);
    }

    private static final class TestConnectorFactory extends AbstractConnectorFactory {
        private ParsedVenueConfig parsedConfig;
        private boolean createSawParsedConfig;

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        protected Object parseVenueConfig(Map<String, Object> rawConfig) {
            parsedConfig = new ParsedVenueConfig((String) rawConfig.get("endpoint"));
            return parsedConfig;
        }

        @Override
        protected Connector createConnector(InstrumentConfig instrument, GatewayConfig config, Object venueConfig) {
            createSawParsedConfig = venueConfig == parsedConfig;
            return new TestConnector(
                    instrument,
                    new RecordingParser(),
                    new RecordingSnapshotStrategy(),
                    new ControlledRecoveryStrategy(),
                    new RecordingSubscriptionBuilder());
        }
    }

    private record ParsedVenueConfig(String endpoint) {
    }
}
