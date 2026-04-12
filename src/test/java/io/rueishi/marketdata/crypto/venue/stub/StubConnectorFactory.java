package io.rueishi.marketdata.crypto.venue.stub;

import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.AbstractConnectorFactory;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Test-only venue provider used to prove ServiceLoader discovery and bootstrap fan-out.
 *
 * <p>The class lives under the test {@code venue} package so production
 * {@code core} code can discover it exactly as it would discover a later real
 * venue: through {@code META-INF/services} and {@link java.util.ServiceLoader}.
 * Its connector and strategies are inert stubs, allowing bootstrap tests to
 * verify construction, initialization, and shutdown without opening network
 * connections or parsing live venue messages.</p>
 */
public final class StubConnectorFactory extends AbstractConnectorFactory {
    private static final List<Integer> CREATED_INSTRUMENT_IDS = new ArrayList<>();
    private static int initCount;
    private static int shutdownCount;

    /**
     * Resets static observation state between tests.
     */
    public static void reset() {
        CREATED_INSTRUMENT_IDS.clear();
        initCount = 0;
        shutdownCount = 0;
    }

    /**
     * Returns the number of connector instances created by this factory.
     *
     * @return connector creation count
     */
    public static int createdCount() {
        return CREATED_INSTRUMENT_IDS.size();
    }

    /**
     * Returns instrument ids observed by factory creation calls.
     *
     * @return copy of created instrument ids
     */
    public static List<Integer> createdInstrumentIds() {
        return List.copyOf(CREATED_INSTRUMENT_IDS);
    }

    /**
     * Returns the number of stub connectors initialized by bootstrap.
     *
     * @return init call count
     */
    public static int initCount() {
        return initCount;
    }

    /**
     * Returns the number of stub connectors shut down through the runtime handle.
     *
     * @return shutdown call count
     */
    public static int shutdownCount() {
        return shutdownCount;
    }

    /**
     * Declares the venue served by the test provider.
     *
     * @return Coinbase L2 venue identity reused for Phase 1 test configuration
     */
    @Override
    public VenueEnum venue() {
        return VenueEnum.COINBASE_L2;
    }

    /**
     * Parses the raw venue config for the stub provider.
     *
     * <p>The stub only verifies that the expected endpoint value is present; it
     * does not retain credentials or perform venue-specific setup.</p>
     *
     * @param rawConfig raw venue config block from bootstrap
     * @return endpoint text used by connector construction
     */
    @Override
    protected Object parseVenueConfig(Map<String, Object> rawConfig) {
        return rawConfig.get("endpoint");
    }

    /**
     * Creates an inert connector for one configured instrument.
     *
     * @param instrument configured instrument
     * @param config validated gateway config
     * @param venueConfig parsed stub endpoint value
     * @return stub connector ready for {@link Connector#init(ConnectorContext)}
     */
    @Override
    protected Connector createConnector(InstrumentConfig instrument, GatewayConfig config, Object venueConfig) {
        CREATED_INSTRUMENT_IDS.add(instrument.instrumentId);
        return new StubConnector(instrument);
    }

    private static final class StubConnector implements Connector {
        private final InstrumentConfig instrument;
        private boolean initialized;

        private StubConnector(InstrumentConfig instrument) {
            this.instrument = Objects.requireNonNull(instrument, "instrument");
        }

        @Override
        public void init(ConnectorContext ctx) {
            Objects.requireNonNull(ctx, "ctx");
            if (initialized) {
                throw new IllegalStateException("stub connector already initialized");
            }
            initialized = true;
            initCount++;
        }

        @Override
        public void connect() {
        }

        @Override
        public void recover(RecoveryRequest request) {
        }

        @Override
        public void shutdown() {
            shutdownCount++;
        }

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        public int instrumentId() {
            return instrument.instrumentId;
        }
    }
}
