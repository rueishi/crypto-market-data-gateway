package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.transport.WebSocketFrameHandler;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Liveness monitoring tests for {@link CoinbaseL2Connector}.
 *
 * <p>The suite uses a real Coinbase L2 connector with an injected mutable
 * nanosecond clock and in-memory publisher/counters. It drives parser ack
 * validation and timeout detection directly, without wall-clock sleeps or an
 * external Coinbase connection, to verify that the connector detects stalled
 * heartbeat state and initiates the same connector-owned recovery path used by
 * parser and downstream reset triggers.</p>
 */
class CoinbaseL2LivenessTest {

    /**
     * Verifies no timeout fires before the Coinbase subscription acknowledgement starts monitoring.
     */
    @Test
    void livenessDoesNotFireBeforeSubscriptionAckValidation() {
        MutableNanoClock clock = new MutableNanoClock(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);
        Fixture fixture = new Fixture(clock, 10);
        try {
            fixture.connector.detectLivenessTimeout();

            assertThat(fixture.connector.livenessMonitoringActive()).isFalse();
            assertThat(fixture.instrumentCounters().heartbeatsMissed().get()).isZero();
            assertThat(fixture.instrumentCounters().livenessFailures().get()).isZero();
            assertThat(fixture.publisher.totalCount()).isZero();
        } finally {
            fixture.close();
        }
    }

    /**
     * Verifies ack validation starts monitoring and a stalled heartbeat requests reset recovery.
     */
    @Test
    void livenessTimeoutUpdatesCountersAndTriggersRecovery() {
        MutableNanoClock clock = new MutableNanoClock(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);
        Fixture fixture = new Fixture(clock, 10);
        try {
            fixture.parse("""
                    {"type":"subscriptions","channels":[
                    {"name":"level2","product_ids":["BTC-USD"]},
                    {"name":"heartbeat","product_ids":["BTC-USD"]}]}""");

            assertThat(fixture.connector.livenessMonitoringActive()).isTrue();
            assertThat(fixture.instrumentCounters().lastHeartbeatReceivedNanos().get())
                    .isEqualTo(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);

            clock.set(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME + 10_000_000L);
            fixture.connector.detectLivenessTimeout();
            waitUntil(() -> fixture.instrumentCounters().recoveryAttempts().get() == 1);

            assertThat(fixture.instrumentCounters().heartbeatsMissed().get()).isEqualTo(1);
            assertThat(fixture.instrumentCounters().livenessFailures().get()).isEqualTo(1);
            assertThat(fixture.instrumentCounters().recoveryAttempts().get()).isEqualTo(1);
            assertThat(fixture.recoveryStrategy.lastRequest.reasonCode)
                    .isEqualTo(RecoveryReasonCode.HEARTBEAT_TIMEOUT);
            assertThat(fixture.recoveryStrategy.lastRequest.requestType)
                    .isEqualTo(RecoveryRequestType.RESET);
            assertThat(fixture.publisher.totalCount()).isEqualTo(1);
        } finally {
            fixture.close();
        }
    }

    /**
     * Verifies repeated heartbeat timer firings are coalesced while recovery remains active.
     */
    @Test
    void repeatedLivenessTimeoutDuringActiveRecoveryIsCoalesced() {
        MutableNanoClock clock = new MutableNanoClock(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);
        Fixture fixture = new Fixture(clock, 10);
        try {
            fixture.parse("""
                    {"type":"subscriptions","channels":[
                    {"name":"level2","product_ids":["BTC-USD"]},
                    {"name":"heartbeat","product_ids":["BTC-USD"]}]}""");

            clock.set(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME + 10_000_000L);
            fixture.connector.detectLivenessTimeout();
            clock.set(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME + 20_000_000L);
            fixture.connector.detectLivenessTimeout();
            waitUntil(() -> fixture.instrumentCounters().recoveryAttempts().get() == 1
                    && fixture.instrumentCounters().recoveryRequestsIgnoredInProgress().get() == 1);

            assertThat(fixture.instrumentCounters().recoveryAttempts().get()).isEqualTo(1);
            assertThat(fixture.instrumentCounters().recoveryRequestsIgnoredInProgress().get()).isEqualTo(1);
            assertThat(fixture.instrumentCounters().heartbeatsMissed().get()).isEqualTo(2);
        } finally {
            fixture.close();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MutableNanoClock clock;
        private final GatewayConfig config;
        private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        private final GatewayCounters gatewayCounters = CoinbaseL2ConnectorTestSupport.gatewayCounters();
        private final InMemoryPublisher publisher = new InMemoryPublisher(8, 4096);
        private final RecordingRecoveryStrategy recoveryStrategy = new RecordingRecoveryStrategy();
        private final CoinbaseL2Connector connector;

        private Fixture(MutableNanoClock clock, long heartbeatTimeoutMs) {
            this.clock = clock;
            this.config = CoinbaseL2ConnectorFactoryTest.validConfig("ws://example.test");
            this.config.transport.heartbeatTimeoutMs = heartbeatTimeoutMs;
            CoinbaseConfig coinbaseConfig = CoinbaseConfig.from(config.venueConfig);
            CoinbaseL2SubscriptionBuilder subscription =
                    new CoinbaseL2SubscriptionBuilder(coinbaseConfig, new CoinbaseAuthenticator(coinbaseConfig), () -> 1L);
            this.connector = new CoinbaseL2Connector(
                    CoinbaseL2ConnectorFactoryTest.instrument("BTC-USD", 1001),
                    new CoinbaseL2FeedParser(CoinbaseL2ConnectorFactoryTest.instrument("BTC-USD", 1001)),
                    new CoinbaseL2SnapshotStrategy(),
                    recoveryStrategy,
                    subscription,
                    coinbaseConfig,
                    config.encoding.maxLevelsPerMessage,
                    config.encoding.bufferHeadroomBytes);
            connector.init(CoinbaseL2ConnectorTestSupport.context(
                    gatewayCounters,
                    publisher,
                    eventLoopGroup,
                    config,
                    clock));
        }

        private void parse(String json) {
            ByteBuf frame = Unpooled.copiedBuffer(json, StandardCharsets.UTF_8);
            try {
                ((WebSocketFrameHandler) connector).onTextFrame(frame);
            } finally {
                frame.release();
            }
        }

        private io.rueishi.marketdata.crypto.core.observability.InstrumentCounters instrumentCounters() {
            return connector.instrumentCounters();
        }

        @Override
        public void close() {
            connector.shutdown();
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
            gatewayCounters.close();
        }
    }

    private static void waitUntil(BooleanSupplier condition) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for liveness recovery", ex);
            }
        }
        throw new AssertionError("Timed out waiting for liveness recovery");
    }

    private static final class RecordingRecoveryStrategy implements RecoveryStrategy {
        private RecoveryRequest lastRequest;

        /**
         * Records the recovery request without completing Phase A so duplicate checks hit base coalescing.
         *
         * @param request recovery request built by the connector
         * @param ctx recovery context supplied by the connector
         */
        @Override
        public void execute(RecoveryRequest request, RecoveryContext ctx) {
            lastRequest = request;
        }
    }

    private static final class MutableNanoClock implements NanoClock {
        private long value;

        private MutableNanoClock(long value) {
            this.value = value;
        }

        private void set(long value) {
            this.value = value;
        }

        @Override
        public long nanoTime() {
            return value;
        }
    }
}
