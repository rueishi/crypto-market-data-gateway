package io.rueishi.marketdata.crypto.core.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestBatchResult;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.SbeRecoveryRequestEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3 integration tests for {@link GatewayRuntime} as created by {@link GatewayBootstrap}.
 *
 * <p>The suite uses real bootstrap validation, mapped observability, metrics
 * endpoint startup, publisher selection, runtime recovery routing, and graceful
 * shutdown while replacing only the venue connector with a recording test
 * double. That keeps the test focused on runtime ownership and lifecycle
 * behavior without opening external transport sockets.</p>
 */
class Phase3GatewayRuntimeIntegrationTest {

    /**
     * Verifies bootstrap returns a live runtime that routes direct and SBE
     * downstream recovery requests before shutdown, rejects recovery after
     * shutdown begins, stops metrics, and flushes mapped observability files.
     */
    @Test
    void bootstrapRuntimeRoutesRecoveryAndDrainsResourcesOnShutdown(@TempDir Path tempDir) {
        RecordingConnectorFactory factory = new RecordingConnectorFactory();
        GatewayConfig config = validConfig(tempDir);
        GatewayRuntime runtime = new GatewayBootstrap(new VenueRegistry(List.of(factory))).initialize(config);

        assertThat(runtime.metricsEndpoint().boundPort()).isGreaterThan(0);
        assertThat(runtime.connectors()).hasSize(1);
        assertThat(factory.connector.initCalls).hasValue(1);
        assertThat(runtime.requestRecovery(recoveryRequest(1001))).isTrue();
        assertThat(factory.connector.recoveryRequests).hasValue(1);
        UnsafeBuffer recoveryControlBuffer = new UnsafeBuffer(new byte[128]);
        int recoveryControlLength = new SbeRecoveryRequestEncoder().encodeRequest(
                recoveryControlBuffer,
                0,
                recoveryRequest(1001));
        RecoveryRequestBatchResult recoveryControlResult =
                runtime.recoveryRequestReceiver().receive(recoveryControlBuffer, 0, recoveryControlLength);
        assertThat(recoveryControlResult.acceptedRequests()).isEqualTo(1);
        assertThat(factory.connector.recoveryRequests).hasValue(2);

        runtime.shutdownGracefully();

        assertThat(factory.connector.shutdownCalls).hasValue(1);
        assertThat(runtime.requestRecovery(recoveryRequest(1001))).isFalse();
        assertThat(runtime.shutdownDeadlineExceeded()).isFalse();
        assertThat(runtime.metricsEndpoint().boundPort()).isZero();
        assertThat(Files.exists(Path.of(config.observability.countersSharedMemoryPath + ".metadata"))).isTrue();
        assertThat(Files.exists(Path.of(config.observability.countersSharedMemoryPath + ".values"))).isTrue();
        assertThat(Files.exists(Path.of(config.observability.errorLogPath))).isTrue();
    }

    private static GatewayConfig validConfig(Path tempDir) {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-phase3-runtime";
        config.environment = "test";
        config.venue = VenueEnum.COINBASE_L2;
        config.instruments = List.of(instrument("BTC-USD", 1001));
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 100;
        config.transport.reconnectBackoffBaseMs = 1;
        config.transport.reconnectBackoffMaxMs = 2;
        config.transport.reconnectBackoffJitterMs = 0;
        config.transport.heartbeatTimeoutMs = 100;
        config.transport.frameSizeLimitBytes = 4096;
        config.transport.shutdownDeadlineMs = 5_000;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 16;
        config.encoding.bufferHeadroomBytes = 0;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = tempDir.resolve("counters").toString();
        config.observability.errorLogPath = tempDir.resolve("errors").toString();
        config.observability.metricsHttpPort = 0;
        config.publisher = new PublisherConfig();
        config.publisher.type = "IN_MEMORY";
        config.venueConfig = new LinkedHashMap<>(Map.of(
                "endpoint", "ws://example.test",
                "apiKey", "test-key",
                "apiSecret", "c2VjcmV0",
                "passphrase", "test-passphrase"));
        return config;
    }

    private static InstrumentConfig instrument(String exchangeSymbol, int instrumentId) {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = exchangeSymbol;
        instrument.instrumentId = instrumentId;
        return instrument;
    }

    private static RecoveryRequest recoveryRequest(int instrumentId) {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                instrumentId,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123L);
    }

    /**
     * Connector factory that supplies a recording connector for runtime-level tests.
     */
    private static final class RecordingConnectorFactory implements ConnectorFactory {
        private final RecordingConnector connector = new RecordingConnector(1001);

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        public Connector create(InstrumentConfig instrument, GatewayConfig config) {
            return connector;
        }
    }

    /**
     * Minimal connector that records bootstrap and runtime lifecycle calls.
     */
    private static final class RecordingConnector implements Connector {
        private final int instrumentId;
        private final AtomicInteger initCalls = new AtomicInteger();
        private final AtomicInteger recoveryRequests = new AtomicInteger();
        private final AtomicInteger shutdownCalls = new AtomicInteger();

        private RecordingConnector(int instrumentId) {
            this.instrumentId = instrumentId;
        }

        @Override
        public void init(ConnectorContext ctx) {
            initCalls.incrementAndGet();
            assertThat(ctx).isNotNull();
        }

        @Override
        public void connect() {
        }

        @Override
        public void recover(RecoveryRequest request) {
            recoveryRequests.incrementAndGet();
        }

        @Override
        public void shutdown() {
            shutdownCalls.incrementAndGet();
        }

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        public int instrumentId() {
            return instrumentId;
        }
    }
}
