package io.rueishi.marketdata.crypto.core.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle tests for {@link AbstractConnector}.
 *
 * <p>These tests use a stub venue connector with no live transport. The parser,
 * snapshot strategy, recovery strategy, subscription builder, publisher, and
 * context are small test doubles so the suite can verify connector-owned
 * context allocation, frame delegation, subscription lifecycle hooks, and the
 * rule that {@link ConnectorContext} is unpacked rather than retained.</p>
 */
class AbstractConnectorLifecycleTest {

    /**
     * Verifies that init allocates one parse context, initial contexts, and no ConnectorContext field.
     */
    @Test
    void initAllocatesConnectorOwnedContextsWithoutRetainingConnectorContext() {
        TestHarness harness = new TestHarness();
        try {
            TestConnector connector = harness.newConnector();

            connector.init(harness.newContext());

            assertThat(connector.parseContextView()).isNotNull();
            assertThat(connector.snapshotContextView()).isNotNull();
            assertThat(connector.recoveryContextView()).isNotNull();
            assertThat(connector.countersView()).isNotNull();
            assertThat(connector.venue()).isEqualTo(VenueEnum.COINBASE_L2);
            assertThat(connector.instrumentId()).isEqualTo(1001);
            assertThat(AbstractConnector.class.getDeclaredFields())
                    .extracting(Field::getType)
                    .noneMatch(ConnectorContext.class::equals);
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies that connect uses the subscription builder and triggers snapshot acquisition.
     */
    @Test
    void connectBuildsSubscribePayloadAndTriggersSnapshotStrategy() {
        TestHarness harness = new TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());

            connector.connect();

            assertThat(connector.subscribePayloads).containsExactly("subscribe:BTC-USD");
            assertThat(harness.snapshot.triggerCount).hasValue(1);
            assertThat(harness.snapshot.lastContext).isSameAs(connector.snapshotContextView());
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies that WebSocket frame callbacks delegate to the parser with the stable parse context.
     */
    @Test
    void frameCallbacksDelegateToParserWithParseContext() {
        TestHarness harness = new TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());
            ParseContext parseContext = connector.parseContextView();

            connector.onTextFrame(Unpooled.wrappedBuffer(new byte[] {1}));
            connector.onBinaryFrame(Unpooled.wrappedBuffer(new byte[] {2}));

            assertThat(harness.parser.textCalls).hasValue(1);
            assertThat(harness.parser.binaryCalls).hasValue(1);
            assertThat(harness.parser.lastContext).isSameAs(parseContext);
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies that lifecycle methods fail clearly before init.
     */
    @Test
    void lifecycleMethodsRequireInit() {
        TestHarness harness = new TestHarness();
        try {
            TestConnector connector = harness.newConnector();

            assertThatThrownBy(connector::connect)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("initialized");
            assertThatThrownBy(() -> connector.onTextFrame(Unpooled.EMPTY_BUFFER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("initialized");
        } finally {
            harness.close();
        }
    }

    static final class TestHarness implements AutoCloseable {
        final EventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(1);
        final GatewayCounters gatewayCounters =
                new GatewayCounters(CoreTestFixtures.newCountersManager(), "gateway-1", "test", "COINBASE_L2");
        final RecordingPublisher publisher = new RecordingPublisher();
        final RecordingParser parser = new RecordingParser();
        final RecordingSnapshotStrategy snapshot = new RecordingSnapshotStrategy();
        final ControlledRecoveryStrategy recovery = new ControlledRecoveryStrategy();
        final RecordingSubscriptionBuilder subscription = new RecordingSubscriptionBuilder();
        final InstrumentConfig instrument = newInstrument();

        TestConnector newConnector() {
            return new TestConnector(instrument, parser, snapshot, recovery, subscription);
        }

        //NanoClock nanoClock =  () -> 123L;
        //CachedNanoClock is class does not inherit from NanoClock, so we need to use 
        // the concrete type here to satisfy the context constructor and allow the 
        // test connector to call advance() on it.

        CachedNanoClock nanoClock = new CachedNanoClock();
        {
            nanoClock.advance(123L);
        }

        EpochClock epochClock = () -> 456L;

        ConnectorContext newContext() {
            return new DefaultConnectorContext(
                    gatewayCounters,
                    publisher,
                    nanoClock,
                    epochClock,
                    new TransportConfig(),
                    eventLoopGroup,
                    Map.of("endpoint", "wss://example.test"));
        }

        @Override
        public void close() {
            gatewayCounters.close();
            eventLoopGroup.shutdownGracefully();
        }

        private static InstrumentConfig newInstrument() {
            InstrumentConfig instrument = new InstrumentConfig();
            instrument.exchangeSymbol = "BTC-USD";
            instrument.instrumentId = 1001;
            return instrument;
        }
    }
}
