package io.rueishi.marketdata.crypto.core.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.ConfigValidator;
import io.rueishi.marketdata.crypto.core.config.CpuAffinityConfig;
import io.rueishi.marketdata.crypto.core.config.CpuAffinityMode;
import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for optional {@link CpuAffinitySupport} and CPU-affinity validation.
 *
 * <p>The tests avoid requiring OpenHFT affinity at compile time. They verify
 * disabled affinity is a no-op, best-effort affinity tolerates an unavailable
 * provider when busy-wait is disabled, and startup validation catches explicit
 * CPU-id mistakes before bootstrap attempts to pin event-loop threads.</p>
 */
class CpuAffinitySupportTest {

    /**
     * Verifies disabled CPU affinity returns a closeable no-op lock.
     *
     * @throws Exception if the returned closeable cannot be closed
     */
    @Test
    void disabledAffinityIsNoop() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            CpuAffinityConfig config = new CpuAffinityConfig();
            config.enabled = false;

            AutoCloseable lock = CpuAffinitySupport.acquire(group, config, false, 0);

            lock.close();
            assertThat(group.isShuttingDown()).isFalse();
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * Verifies best-effort affinity does not fail startup when the optional provider is absent.
     *
     * @throws Exception if the no-op lock cannot be closed
     */
    @Test
    void bestEffortAffinityFallsBackWhenProviderUnavailable() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            CpuAffinityConfig config = new CpuAffinityConfig();
            config.enabled = true;
            config.mode = CpuAffinityMode.AUTO;

            AutoCloseable lock = CpuAffinitySupport.acquire(group, config, false, 0);

            lock.close();
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * Verifies config validation rejects invalid explicit affinity and busy-wait without affinity.
     */
    @Test
    void validatorRejectsInvalidAffinityConfiguration() {
        GatewayConfig duplicateCpuIds = validConfig();
        duplicateCpuIds.transport.cpuAffinity.enabled = true;
        duplicateCpuIds.transport.cpuAffinity.mode = CpuAffinityMode.EXPLICIT;
        duplicateCpuIds.transport.cpuAffinity.eventLoopCpuIds = List.of(2, 2);

        assertThatThrownBy(() -> ConfigValidator.validate(duplicateCpuIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");

        GatewayConfig busyWaitWithoutAffinity = validConfig();
        busyWaitWithoutAffinity.transport.busyWaitEnabled = true;
        busyWaitWithoutAffinity.transport.cpuAffinity.enabled = false;

        assertThatThrownBy(() -> ConfigValidator.validate(busyWaitWithoutAffinity))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("busyWaitEnabled");
    }

    private static GatewayConfig validConfig() {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-affinity-test";
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
        config.transport.shutdownDeadlineMs = 100;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 16;
        config.encoding.bufferHeadroomBytes = 0;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = "target/test-affinity-counters";
        config.observability.errorLogPath = "target/test-affinity-errors";
        config.observability.metricsHttpPort = 0;
        config.publisher = new PublisherConfig();
        config.publisher.type = "IN_MEMORY";
        config.venueConfig = Map.of(
                "endpoint", "ws://example.test",
                "apiKey", "test-key",
                "apiSecret", "c2VjcmV0",
                "passphrase", "test-passphrase");
        return config;
    }
}
