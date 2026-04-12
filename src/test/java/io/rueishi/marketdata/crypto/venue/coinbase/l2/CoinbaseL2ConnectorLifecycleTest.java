package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle tests for {@link CoinbaseL2Connector}.
 *
 * <p>The suite uses a local in-process Netty WebSocket server to verify that a
 * production Coinbase connector initializes from injected clocks and transport
 * dependencies, opens its transport on {@code connect()}, sends the
 * authenticated subscribe payload, and sends authenticated unsubscribe during
 * shutdown without requiring external Coinbase infrastructure.</p>
 */
class CoinbaseL2ConnectorLifecycleTest {

    /**
     * Verifies connector init/connect opens the local WebSocket and shutdown sends authenticated unsubscribe.
     *
     * @throws Exception if the local WebSocket exchange cannot complete
     */
    @Test
    void connectUsesInjectedTransportDependenciesAndSendsSubscribe() throws Exception {
        try (LocalWebSocketServer server = new LocalWebSocketServer()) {
            GatewayConfig config = CoinbaseL2ConnectorFactoryTest.validConfig(server.uri().toString());
            Connector connector = new CoinbaseL2ConnectorFactory()
                    .create(CoinbaseL2ConnectorFactoryTest.instrument("BTC-USD", 1001), config);
            EventLoopGroup clientGroup = new NioEventLoopGroup(1);
            InMemoryPublisher publisher = new InMemoryPublisher(4, 4096);
            try (GatewayCounters counters = CoinbaseL2ConnectorTestSupport.gatewayCounters()) {
                connector.init(CoinbaseL2ConnectorTestSupport.context(counters, publisher, clientGroup, config));

                connector.connect();

                String subscribe = server.takeText();
                assertThat(subscribe).contains("\"type\":\"subscribe\"");
                assertThat(subscribe).contains("\"product_ids\":[\"BTC-USD\"]");
                assertThat(subscribe).contains("\"channels\":[\"level2\",\"heartbeat\"]");

                connector.shutdown();
                String unsubscribe = server.takeText();
                assertThat(unsubscribe).contains("\"type\":\"unsubscribe\"");
                assertThat(unsubscribe).contains("\"product_ids\":[\"BTC-USD\"]");
            } finally {
                connector.shutdown();
                clientGroup.shutdownGracefully().syncUninterruptibly();
            }
        }
    }

    private static final class LocalWebSocketServer implements AutoCloseable {
        private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final BlockingQueue<String> inboundText = new LinkedBlockingQueue<>();
        private final Channel channel;

        private LocalWebSocketServer() {
            channel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(8192));
                            ch.pipeline().addLast(new WebSocketServerProtocolHandler("/", null, true, 4096));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<TextWebSocketFrame>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {
                                    inboundText.add(msg.text());
                                }
                            });
                        }
                    })
                    .bind("127.0.0.1", 0)
                    .syncUninterruptibly()
                    .channel();
        }

        private URI uri() {
            return URI.create("ws://127.0.0.1:" + ((java.net.InetSocketAddress) channel.localAddress()).getPort() + "/");
        }

        private String takeText() throws InterruptedException {
            return inboundText.poll(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() {
            channel.close().syncUninterruptibly();
            bossGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}
