package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.publisher.LoggingPublisher;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import io.rueishi.marketdata.crypto.core.transport.PipelineCustomizer;
import io.rueishi.marketdata.crypto.core.transport.WebSocketFrameHandler;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for P1-000a compile-time interfaces and placeholders.
 *
 * <p>This suite verifies the Phase 1 placeholder boundary types that downstream
 * task cards compile against before full parser, subscription, transport,
 * and logging publisher behavior exists. Metrics endpoint assertions now cover
 * the Phase 3 replacement for the original placeholder. Tests use an in-memory
 * Agrona {@link CountersManager}, Netty heap buffers for harmless contract
 * invocation, and anonymous interface implementations instead of mocking
 * libraries so no production transport or metrics implementation leaks into
 * parser hot paths.</p>
 */
class Phase1PlaceholderContractsTest {

    /**
     * Verifies that every P1-000a support type can be loaded and that deferred
     * placeholders still advertise unsupported behavior where Phase 3 has not
     * replaced them.
     */
    @Test
    void placeholderSupportTypesExistForPhaseOneDependents() {
        assertThat(MetricsEndpoint.class).isNotNull();
        assertThat(FeedParser.class).isInterface();
        assertThat(SubscriptionBuilder.class).isInterface();
        assertThat(WebSocketFrameHandler.class).isInterface();
        assertThat(PipelineCustomizer.class).isInterface();
        assertThat(LoggingPublisher.class).isNotNull();

        CountersManager manager = GatewayCountersTest.newCountersManager();
        try (MetricsEndpoint endpoint = new MetricsEndpoint(manager, 0)) {
            assertThat(endpoint.countersReader()).isSameAs(manager);
            assertThat(endpoint.httpPort()).isZero();
            endpoint.start();
            endpoint.stop();
        }

        try (LoggingPublisher publisher = new LoggingPublisher()) {
            assertThatThrownBy(publisher::publishUnsupported)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("deferred beyond P1-000a");
        }
    }

    /**
     * Verifies that the FeedParser default binary-frame path records unknown
     * binary messages without retaining parser session state.
     */
    @Test
    void feedParserDefaultBinaryFrameCountsUnknownMessages() {
        try (GatewayCounters gatewayCounters =
                     new GatewayCounters(GatewayCountersTest.newCountersManager(), "gateway-1", "test", "STUB")) {
            InstrumentCounters counters = gatewayCounters.forInstrument(1001);
            FeedParser parser = (frame, ctx) -> {
            };
            ParseContext ctx = new DefaultParseContext(
                    new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0),
                    new TestPublisher(),
                    counters,
                    () -> 0L);
            ByteBuf frame = Unpooled.directBuffer(0);

            try {
                parser.onBinaryFrame(frame, ctx);

                assertThat(counters.unknownTypeDrops().get()).isEqualTo(1);
            } finally {
                frame.release();
            }
        }
    }

    /**
     * Verifies that the placeholder interfaces expose the exact method shapes
     * needed by later connector, parser, and subscription cards.
     *
     * @throws Exception if a required method cannot be reflected from the interface
     */
    @Test
    void placeholderInterfacesExposeExpectedCompileTimeMethodShapes() throws Exception {
        assertThat(FeedParser.class.getMethod("onTextFrame", ByteBuf.class, ParseContext.class).getReturnType())
                .isEqualTo(Void.TYPE);
        assertThat(FeedParser.class.getMethod("onBinaryFrame", ByteBuf.class, ParseContext.class).getReturnType())
                .isEqualTo(Void.TYPE);
        assertThat(SubscriptionBuilder.class.getMethod("buildSubscribe", InstrumentConfig.class).getReturnType())
                .isEqualTo(byte[].class);
        assertThat(SubscriptionBuilder.class.getMethod("buildUnsubscribe", InstrumentConfig.class).getReturnType())
                .isEqualTo(byte[].class);
        assertThat(WebSocketFrameHandler.class.getMethod("onTextFrame", ByteBuf.class).getReturnType())
                .isEqualTo(Void.TYPE);
        assertThat(WebSocketFrameHandler.class.getMethod("onBinaryFrame", ByteBuf.class).getReturnType())
                .isEqualTo(Void.TYPE);
        assertThat(WebSocketFrameHandler.class.getMethod("onDisconnected", Throwable.class).getReturnType())
                .isEqualTo(Void.TYPE);
        assertThat(PipelineCustomizer.class.getMethod("customize", ChannelPipeline.class).getReturnType())
                .isEqualTo(Void.TYPE);
    }

    /**
     * Verifies that P1-000a parser state rules remain visible at the interface boundary after P2 transport exists.
     */
    @Test
    void placeholderBoundariesDoNotIntroduceParserState() {
        assertThat(FeedParser.class.getDeclaredFields())
                .filteredOn(field -> !field.isSynthetic())
                .filteredOn(field -> !isInterfaceConstant(field))
                .isEmpty();
    }

    /**
     * Verifies that P1-000a does not add legacy transport variants and that
     * parser state rules remain visible at the interface boundary.
     */
    @Test
    void placeholderBoundariesDoNotIntroduceLegacyTransportVariants() {
        assertThat(classExists("io.rueishi.marketdata.crypto.core.transport.EpollWebSocketTransport")).isFalse();
    }

    private static boolean classExists(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }

    private static boolean isInterfaceConstant(Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers);
    }

    private static final class TestPublisher implements Publisher {
        @Override
        public boolean publish(
                DirectBuffer buffer,
                int offset,
                int length,
                InstrumentCounters counters,
                NanoClock nanoClock) {
            return true;
        }

        @Override
        public void publishReset(
                int instrumentId,
                byte venueByte,
                byte bookDepthByte,
                byte templateIdByte,
                NanoClock nanoClock) {
        }
    }
}
