package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Phase 2 regression tests for Coinbase live-path observability counters.
 *
 * <p>The tests use the real in-memory Agrona counter manager from
 * {@link GatewayCountersTest} rather than mocks. They verify that P2-000's
 * shared counter surface already exposes the accessors needed by the later
 * Coinbase L2 parser, liveness, subscription-ack validation, and connection
 * gauge cards.</p>
 */
class Phase2InstrumentCountersTest {

    /**
     * Verifies the Phase 2 live Coinbase path has heartbeat, unknown-type, and validation counters.
     */
    @Test
    void exposesPhaseTwoLivePathInstrumentCounters() {
        try (GatewayCounters gatewayCounters =
                     new GatewayCounters(GatewayCountersTest.newCountersManager(), "gateway-1", "test", "COINBASE_L2")) {
            InstrumentCounters counters = gatewayCounters.forInstrument(1001);

            counters.heartbeatsReceived().increment();
            counters.unknownTypeDrops().increment();
            counters.productIdMismatches().increment();
            counters.unknownSymbolDrops().increment();
            counters.subscriptionValidationFailures().increment();
            counters.lastHeartbeatReceivedNanos().set(123_456_789L);
            counters.lastMessageReceivedNanos().set(123_456_790L);

            assertThat(counters.heartbeatsReceived().get()).isEqualTo(1);
            assertThat(counters.unknownTypeDrops().get()).isEqualTo(1);
            assertThat(counters.productIdMismatches().get()).isEqualTo(1);
            assertThat(counters.unknownSymbolDrops().get()).isEqualTo(1);
            assertThat(counters.subscriptionValidationFailures().get()).isEqualTo(1);
            assertThat(counters.lastHeartbeatReceivedNanos().get()).isEqualTo(123_456_789L);
            assertThat(counters.lastMessageReceivedNanos().get()).isEqualTo(123_456_790L);
        }
    }

    /**
     * Verifies the gateway-level active connection gauge is available for Phase 2 connector liveness.
     */
    @Test
    void exposesPhaseTwoActiveConnectionGauge() {
        try (GatewayCounters gatewayCounters =
                     new GatewayCounters(GatewayCountersTest.newCountersManager(), "gateway-1", "test", "COINBASE_L2")) {
            gatewayCounters.activeConnections().increment();

            assertThat(gatewayCounters.activeConnections().get()).isEqualTo(1);
            assertThat(gatewayCounters.activeConnections().label())
                    .contains("gateway_active_connections")
                    .contains("venue=COINBASE_L2");
        }
    }
}
