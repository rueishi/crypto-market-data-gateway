package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecoveryRequestRouter}.
 *
 * <p>The suite uses tiny recording {@link Connector} stubs instead of real
 * venue connectors so routing behavior can be tested independently from
 * Coinbase recovery strategy work. Covered scenarios include exact
 * venue/instrument routing, mismatched venue and unknown instrument ignores,
 * duplicate route-key rejection, and delegation through the connector's
 * {@link Connector#requestRecovery(RecoveryRequest)} scheduling entry point.</p>
 */
class RecoveryRequestRouterTest {

    /**
     * Verifies that an exact venue/instrument match invokes only the owning connector.
     */
    @Test
    void routesMatchingRequestToOwningConnectorOnly() {
        RecordingConnector btc = new RecordingConnector(VenueEnum.COINBASE_L2, 1001);
        RecordingConnector eth = new RecordingConnector(VenueEnum.COINBASE_L2, 1002);
        RecoveryRequest request = request(VenueEnum.COINBASE_L2, 1002);

        boolean routed = new RecoveryRequestRouter(List.of(btc, eth)).route(request);

        assertThat(routed).isTrue();
        assertThat(btc.requestRecoveryCount).hasValue(0);
        assertThat(eth.requestRecoveryCount).hasValue(1);
        assertThat(eth.lastRequest).hasValue(request);
    }

    /**
     * Verifies that cross-venue requests and unknown instruments do not affect
     * any connector in this runtime.
     */
    @Test
    void ignoresMismatchedVenueAndUnknownInstrument() {
        RecordingConnector connector = new RecordingConnector(VenueEnum.COINBASE_L2, 1001);
        RecoveryRequestRouter router = new RecoveryRequestRouter(List.of(connector));

        assertThat(router.route(request(VenueEnum.BINANCE_L2, 1001))).isFalse();
        assertThat(router.route(request(VenueEnum.COINBASE_L2, 9999))).isFalse();

        assertThat(connector.requestRecoveryCount).hasValue(0);
    }

    /**
     * Verifies that bootstrap-style duplicate connector routes are rejected
     * rather than letting later requests route ambiguously.
     */
    @Test
    void rejectsDuplicateConnectorRoutes() {
        RecordingConnector first = new RecordingConnector(VenueEnum.COINBASE_L2, 1001);
        RecordingConnector second = new RecordingConnector(VenueEnum.COINBASE_L2, 1001);

        assertThatThrownBy(() -> new RecoveryRequestRouter(List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate connector route");
    }

    private static RecoveryRequest request(VenueEnum venue, int instrumentId) {
        return new RecoveryRequest(
                venue,
                instrumentId,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123L);
    }

    private static final class RecordingConnector implements Connector {
        private final VenueEnum venue;
        private final int instrumentId;
        private final AtomicInteger requestRecoveryCount = new AtomicInteger();
        private final AtomicReference<RecoveryRequest> lastRequest = new AtomicReference<>();

        private RecordingConnector(VenueEnum venue, int instrumentId) {
            this.venue = venue;
            this.instrumentId = instrumentId;
        }

        @Override
        public void init(ConnectorContext ctx) {
        }

        @Override
        public void connect() {
        }

        @Override
        public void recover(RecoveryRequest request) {
            lastRequest.set(request);
        }

        @Override
        public void requestRecovery(RecoveryRequest request) {
            requestRecoveryCount.incrementAndGet();
            recover(request);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public VenueEnum venue() {
            return venue;
        }

        @Override
        public int instrumentId() {
            return instrumentId;
        }
    }
}
