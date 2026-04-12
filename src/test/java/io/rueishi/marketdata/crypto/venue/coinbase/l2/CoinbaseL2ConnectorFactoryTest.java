package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.DefaultConnectorContext;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.transport.WebSocketFrameHandler;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Factory wiring tests for {@link CoinbaseL2ConnectorFactory}.
 *
 * <p>The suite verifies that the factory parses raw Coinbase configuration,
 * reports the correct venue, constructs real Coinbase L2 connectors, rejects
 * malformed venue config early, and creates independent connector instances
 * for separate instruments. Runtime dependencies are in-memory counters,
 * publishers, and NIO event loop groups; no live external network is used.</p>
 */
class CoinbaseL2ConnectorFactoryTest {

    /**
     * Verifies the factory parses Coinbase config and returns a Coinbase L2 connector for one instrument.
     */
    @Test
    void createParsesVenueConfigAndBuildsCoinbaseL2Connector() {
        CoinbaseL2ConnectorFactory factory = new CoinbaseL2ConnectorFactory();

        Connector connector = factory.create(instrument("BTC-USD", 1001), validConfig("ws://example.test"));

        assertThat(factory.venue()).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(connector).isInstanceOf(CoinbaseL2Connector.class);
        assertThat(connector.venue()).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(connector.instrumentId()).isEqualTo(1001);
    }

    /**
     * Verifies malformed raw Coinbase config fails during factory creation rather than at connection time.
     */
    @Test
    void createRejectsMalformedVenueConfig() {
        GatewayConfig config = validConfig("ws://example.test");
        config.venueConfig.remove("apiSecret");

        assertThatThrownBy(() -> new CoinbaseL2ConnectorFactory().create(instrument("BTC-USD", 1001), config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("apiSecret");
    }

    /**
     * Verifies separate instrument connectors keep parser session state isolated after initialization.
     */
    @Test
    void createOneConnectorPerInstrumentWithIsolatedSessionState() {
        CoinbaseL2ConnectorFactory factory = new CoinbaseL2ConnectorFactory();
        GatewayConfig config = validConfig("ws://example.test");
        Connector btc = factory.create(instrument("BTC-USD", 1001), config);
        Connector eth = factory.create(instrument("ETH-USD", 1002), config);
        InMemoryPublisher btcPublisher = new InMemoryPublisher(4, 4096);
        InMemoryPublisher ethPublisher = new InMemoryPublisher(4, 4096);
        EventLoopGroup btcGroup = new NioEventLoopGroup(1);
        EventLoopGroup ethGroup = new NioEventLoopGroup(1);
        try (GatewayCounters btcCounters = CoinbaseL2ConnectorTestSupport.gatewayCounters();
                GatewayCounters ethCounters = CoinbaseL2ConnectorTestSupport.gatewayCounters()) {
            btc.init(CoinbaseL2ConnectorTestSupport.context(btcCounters, btcPublisher, btcGroup, config));
            eth.init(CoinbaseL2ConnectorTestSupport.context(ethCounters, ethPublisher, ethGroup, config));

            parse((WebSocketFrameHandler) btc,
                    "{\"type\":\"snapshot\",\"product_id\":\"BTC-USD\",\"bids\":[],\"asks\":[]}");
            parse((WebSocketFrameHandler) eth,
                    "{\"type\":\"snapshot\",\"product_id\":\"ETH-USD\",\"bids\":[],\"asks\":[]}");

            CoinbaseL2ParserTestSupport.DecodedMessage btcDecoded =
                    new CoinbaseL2ParserTestSupport.DecodedMessage(btcPublisher.lastMessage());
            CoinbaseL2ParserTestSupport.DecodedMessage ethDecoded =
                    new CoinbaseL2ParserTestSupport.DecodedMessage(ethPublisher.lastMessage());
            assertThat(btcDecoded.instrumentId()).isEqualTo(1001);
            assertThat(ethDecoded.instrumentId()).isEqualTo(1002);
            assertThat(btcDecoded.gatewayMessageSeq()).isEqualTo(1);
            assertThat(ethDecoded.gatewayMessageSeq()).isEqualTo(1);
        } finally {
            btcGroup.shutdownGracefully().syncUninterruptibly();
            ethGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    static GatewayConfig validConfig(String endpoint) {
        GatewayConfig config = new GatewayConfig();
        config.venue = VenueEnum.COINBASE_L2;
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 5000;
        config.transport.reconnectBackoffBaseMs = 10;
        config.transport.reconnectBackoffMaxMs = 40;
        config.transport.reconnectBackoffJitterMs = 0;
        config.transport.frameSizeLimitBytes = 4096;
        config.transport.heartbeatTimeoutMs = 10_000;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 16;
        config.encoding.bufferHeadroomBytes = 0;
        config.venueConfig = new LinkedHashMap<>(Map.of(
                "endpoint", endpoint,
                "apiKey", "test-key",
                "apiSecret", "c2VjcmV0",
                "passphrase", "test-passphrase"));
        return config;
    }

    static InstrumentConfig instrument(String exchangeSymbol, int instrumentId) {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = exchangeSymbol;
        instrument.instrumentId = instrumentId;
        return instrument;
    }

    private static void parse(WebSocketFrameHandler handler, String json) {
        ByteBuf frame = Unpooled.copiedBuffer(json, StandardCharsets.UTF_8);
        try {
            handler.onTextFrame(frame);
        } finally {
            frame.release();
        }
    }
}
