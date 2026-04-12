package io.rueishi.marketdata.crypto.core.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.observability.MetricsEndpoint;
import io.rueishi.marketdata.crypto.core.observability.ObservabilityRuntime;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Shutdown lifecycle tests for {@link GatewayRuntime}.
 *
 * <p>The tests use real observability mappings, a real metrics endpoint object,
 * and Netty event-loop groups, while replacing venue connectors with small test
 * doubles. This isolates runtime-owned behavior: recovery rejection during
 * drain, connector shutdown ordering, affinity lock release, event-loop
 * deadline enforcement, and mapped observability cleanup.</p>
 */
class GatewayRuntimeShutdownTest {

    /**
     * Verifies graceful shutdown invokes connectors, releases locks, stops event loops, and rejects new recovery.
     */
    @Test
    void shutdownGracefullyStopsResourcesAndRejectsRecovery(@TempDir Path tempDir) {
        GatewayConfig config = validConfig(tempDir, 5_000L);
        ObservabilityRuntime observability = ObservabilityRuntime.create(
                config.observability,
                config.instanceId,
                config.environment,
                config.venue.name());
        MetricsEndpoint metricsEndpoint = new MetricsEndpoint(
                observability.countersReader(),
                0,
                observability.errorLog());
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        RecordingConnector connector = new RecordingConnector(1001);
        AtomicInteger affinityCloseCount = new AtomicInteger();
        GatewayRuntime runtime = new GatewayRuntime(
                config,
                observability,
                metricsEndpoint,
                new InMemoryPublisher(),
                List.of(connector),
                List.of(eventLoopGroup),
                List.of(affinityCloseCount::incrementAndGet));

        assertThat(runtime.requestRecovery(recoveryRequest())).isTrue();

        runtime.shutdownGracefully();
        runtime.shutdownGracefully();

        assertThat(connector.shutdownCalls).hasValue(1);
        assertThat(connector.recoverCalls).hasValue(1);
        assertThat(affinityCloseCount).hasValue(1);
        assertThat(runtime.requestRecovery(recoveryRequest())).isFalse();
        assertThat(eventLoopGroup.isShuttingDown()).isTrue();
    }

    /**
     * Verifies shutdown stops waiting and records a deadline breach when the event loop is blocked.
     */
    @Test
    void shutdownGracefullyHonorsDeadlineForBlockedEventLoop(@TempDir Path tempDir) {
        GatewayConfig config = validConfig(tempDir, 1L);
        ObservabilityRuntime observability = ObservabilityRuntime.create(
                config.observability,
                config.instanceId,
                config.environment,
                config.venue.name());
        MetricsEndpoint metricsEndpoint = new MetricsEndpoint(
                observability.countersReader(),
                0,
                observability.errorLog());
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        eventLoopGroup.next().execute(() -> {
            try {
                Thread.sleep(200L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
        GatewayRuntime runtime = new GatewayRuntime(
                config,
                observability,
                metricsEndpoint,
                new InMemoryPublisher(),
                List.of(new RecordingConnector(1001)),
                List.of(eventLoopGroup),
                List.of());

        runtime.shutdownGracefully();

        assertThat(runtime.shutdownDeadlineExceeded()).isTrue();
        eventLoopGroup.terminationFuture().awaitUninterruptibly(1, TimeUnit.SECONDS);
    }

    private static GatewayConfig validConfig(Path tempDir, long shutdownDeadlineMs) {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-shutdown-test";
        config.environment = "test";
        config.venue = VenueEnum.COINBASE_L2;
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = "BTC-USD";
        instrument.instrumentId = 1001;
        config.instruments = List.of(instrument);
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 100;
        config.transport.reconnectBackoffBaseMs = 1;
        config.transport.reconnectBackoffMaxMs = 2;
        config.transport.reconnectBackoffJitterMs = 0;
        config.transport.heartbeatTimeoutMs = 100;
        config.transport.frameSizeLimitBytes = 4096;
        config.transport.shutdownDeadlineMs = shutdownDeadlineMs;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 16;
        config.encoding.bufferHeadroomBytes = 0;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = tempDir.resolve("counters").toString();
        config.observability.errorLogPath = tempDir.resolve("errors").toString();
        config.observability.metricsHttpPort = 0;
        config.publisher = new PublisherConfig();
        config.publisher.type = "IN_MEMORY";
        config.venueConfig = java.util.Map.of("endpoint", "ws://example.test");
        return config;
    }

    private static RecoveryRequest recoveryRequest() {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                1L);
    }

    private static final class RecordingConnector implements Connector {
        private final int instrumentId;
        private final AtomicInteger shutdownCalls = new AtomicInteger();
        private final AtomicInteger recoverCalls = new AtomicInteger();

        private RecordingConnector(int instrumentId) {
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
            recoverCalls.incrementAndGet();
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
