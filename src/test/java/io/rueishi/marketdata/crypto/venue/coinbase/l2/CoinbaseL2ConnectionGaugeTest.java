package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * Connection gauge tests for Coinbase connector-owned transport callbacks.
 *
 * <p>The suite uses a local in-process WebSocket server so the production
 * Coinbase L2 connector can establish and close a real Netty transport without
 * contacting Coinbase. It verifies that the gateway-level active connection
 * gauge and connection outcome counters are updated by connector code, not the
 * shared transport.</p>
 */
class CoinbaseL2ConnectionGaugeTest {

    /**
     * Verifies active connection gauge increments on connect and decrements on shutdown.
     *
     * @throws Exception if the local WebSocket server cannot complete the connection
     */
    @Test
    void activeConnectionGaugeTracksConnectAndShutdown() throws Exception {
        try (LocalWebSocketServer server = new LocalWebSocketServer()) {
            GatewayConfig config = CoinbaseL2ConnectorFactoryTest.validConfig(server.uri().toString());
            CoinbaseL2Connector connector = (CoinbaseL2Connector) new CoinbaseL2ConnectorFactory()
                    .create(CoinbaseL2ConnectorFactoryTest.instrument("BTC-USD", 1001), config);
            EventLoopGroup clientGroup = new NioEventLoopGroup(1);
            InMemoryPublisher publisher = new InMemoryPublisher(4, 4096);
            try (GatewayCounters counters = CoinbaseL2ConnectorTestSupport.gatewayCounters()) {
                connector.init(CoinbaseL2ConnectorTestSupport.context(counters, publisher, clientGroup, config));

                connector.connect();

                assertThat(counters.connectionAttempts().get()).isEqualTo(1);
                assertThat(counters.connectionSuccesses().get()).isEqualTo(1);
                assertThat(counters.connectionFailures().get()).isZero();
                assertThat(counters.activeConnections().get()).isEqualTo(1);

                connector.shutdown();
                assertThat(counters.activeConnections().get()).isZero();
            } finally {
                clientGroup.shutdownGracefully().syncUninterruptibly();
            }
        }
    }

    private static final class LocalWebSocketServer implements AutoCloseable {
        private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
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
                                protected void channelRead0(
                                        io.netty.channel.ChannelHandlerContext ctx,
                                        TextWebSocketFrame msg) {
                                    // The test only needs handshake and inbound subscribe acceptance.
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

        @Override
        public void close() {
            channel.close().syncUninterruptibly();
            bossGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}
