package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GatewayCounters}.
 *
 * <p>These tests verify the gateway-scoped counter facade that bootstrap creates
 * before connector initialization. The suite checks that the facade allocates
 * real Agrona {@link AtomicCounter} objects, gives callers stable accessor
 * references, labels counters with gateway dimensions, creates
 * {@link InstrumentCounters} bundles for connector initialization, and rejects
 * invalid startup inputs before hot-path code can observe bad labels. Phase 3
 * also verifies that the facade exposes generic reader access over the same
 * manager that allocated gateway and instrument counters.</p>
 */
class GatewayCountersTest {

    /**
     * Verifies that gateway accessors expose real Agrona counters and that
     * callers can mutate them with the expected Agrona counter operations.
     */
    @Test
    void allocatesGatewayCountersBackedByAgronaAtomicCounters() {
        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            assertThat(counters.activeConnections()).isInstanceOf(AtomicCounter.class);
            assertThat(counters.connectionAttempts()).isInstanceOf(AtomicCounter.class);
            assertThat(counters.connectionSuccesses()).isInstanceOf(AtomicCounter.class);
            assertThat(counters.connectionFailures()).isInstanceOf(AtomicCounter.class);
            assertThat(counters.reconnectAttempts()).isInstanceOf(AtomicCounter.class);

            counters.activeConnections().increment();
            counters.connectionAttempts().getAndAdd(2);

            assertThat(counters.activeConnections().get()).isEqualTo(1);
            assertThat(counters.connectionAttempts().get()).isEqualTo(2);
        }
    }

    /**
     * Verifies that gateway counter accessors return the constructor-allocated
     * counters rather than allocating new counters per call.
     */
    @Test
    void gatewayCounterAccessorsReturnStableReferences() {
        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            assertThat(counters.activeConnections()).isSameAs(counters.activeConnections());
            assertThat(counters.connectionAttempts()).isSameAs(counters.connectionAttempts());
            assertThat(counters.connectionSuccesses()).isSameAs(counters.connectionSuccesses());
            assertThat(counters.connectionFailures()).isSameAs(counters.connectionFailures());
            assertThat(counters.reconnectAttempts()).isSameAs(counters.reconnectAttempts());
        }
    }

    /**
     * Verifies that gateway-level labels include the operational dimensions
     * needed to identify a running process in metrics tooling.
     */
    @Test
    void labelsIncludeGatewayDimensions() {
        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            assertThat(counters.activeConnections().label())
                    .contains("gateway_active_connections")
                    .contains("instanceId=gateway-1")
                    .contains("environment=test")
                    .contains("venue=STUB");
        }
    }

    /**
     * Verifies that each connector receives a fresh per-instrument bundle so
     * bundle caching remains a connector lifecycle decision, not facade state.
     */
    @Test
    void createsFreshPerInstrumentCounterSetsForCallersToCache() {
        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            InstrumentCounters first = counters.forInstrument(1001);
            InstrumentCounters second = counters.forInstrument(1001);

            assertThat(first).isNotSameAs(second);
            assertThat(first.framesReceived()).isNotSameAs(second.framesReceived());
            assertThat(first.framesReceived().label()).contains("instrumentId=1001");
        }
    }

    /**
     * Verifies that generic metrics discovery can iterate both gateway and
     * instrument counters through the reader exposed by the facade, without
     * endpoint-specific registration.
     */
    @Test
    void exposesGenericReaderForGatewayAndInstrumentCounters() {
        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            counters.connectionAttempts().increment();
            InstrumentCounters instrumentCounters = counters.forInstrument(1001);
            instrumentCounters.framesReceived().set(7);

            assertThat(counters.countersReader()).isSameAs(counters.countersManager());
            assertThat(counters.countersReader().getCounterValue(counters.connectionAttempts().id())).isEqualTo(1);
            assertThat(counters.countersReader().getCounterValue(instrumentCounters.framesReceived().id())).isEqualTo(7);
        }
    }

    /**
     * Verifies that invalid startup labels and invalid instrument ids fail
     * before counters with ambiguous labels can be allocated.
     */
    @Test
    void rejectsInvalidConstructionInputs() {
        assertThatThrownBy(() -> new GatewayCounters(newCountersManager(), "", "test", "STUB"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instanceId");

        try (GatewayCounters counters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB")) {
            assertThatThrownBy(() -> counters.forInstrument(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("instrumentId");
        }
    }

    /**
     * Creates an in-memory Agrona counter manager for tests.
     *
     * @return a {@link CountersManager} backed by direct buffers sized for this suite
     */
    static CountersManager newCountersManager() {
        int maxCounters = 256;
        return new CountersManager(
                new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.METADATA_LENGTH * maxCounters)),
                new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.COUNTER_LENGTH * maxCounters)));
    }
}
