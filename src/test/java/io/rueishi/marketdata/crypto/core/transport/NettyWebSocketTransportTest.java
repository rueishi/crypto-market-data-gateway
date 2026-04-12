package io.rueishi.marketdata.crypto.core.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Integration-style tests for {@link NettyWebSocketTransport}.
 *
 * <p>The suite uses a local in-process Netty WebSocket server rather than
 * mocking Netty internals. It verifies the P2-001 lifecycle path of connecting,
 * completing the WebSocket handshake, delivering inbound text frames to a
 * {@link WebSocketFrameHandler}, sending outbound text payloads, closing the
 * channel, and rejecting sends before a connection exists.</p>
 */
class NettyWebSocketTransportTest {

    /**
     * Verifies connect, server-to-client frame dispatch, client-to-server send, and close behavior.
     *
     * @throws Exception if the local server or transport cannot complete the test exchange
     */
    @Test
    void connectSendAndCloseThroughLocalWebSocketServer() throws Exception {
        try (LocalWebSocketServer server = new LocalWebSocketServer()) {
            RecordingFrameHandler handler = new RecordingFrameHandler();
            EventLoopGroup clientGroup = new NioEventLoopGroup(1);
            try {
                NettyWebSocketTransport transport = new NettyWebSocketTransport(
                        clientGroup,
                        server.uri(),
                        null,
                        pipeline -> {
                        },
                        1024,
                        5000,
                        handler);

                transport.connect();
                assertThat(transport.isConnected()).isTrue();
                assertThat(handler.takeText()).isEqualTo("server-ready");

                transport.sendText("subscribe".getBytes(StandardCharsets.UTF_8));
                assertThat(server.takeText()).isEqualTo("subscribe");

                transport.close();
                transport.eventLoop().submit(() -> null).get(5, TimeUnit.SECONDS);
            } finally {
                clientGroup.shutdownGracefully().syncUninterruptibly();
            }
        }
    }

    /**
     * Verifies outbound sends fail clearly when no WebSocket channel is connected.
     */
    @Test
    void sendTextRequiresConnectedChannel() {
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        try {
            NettyWebSocketTransport transport = new NettyWebSocketTransport(
                    clientGroup,
                    URI.create("ws://127.0.0.1:65530/"),
                    null,
                    pipeline -> {
                    },
                    1024,
                    100,
                    new RecordingFrameHandler());

            assertThatThrownBy(() -> transport.sendText(new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not connected");
        } finally {
            clientGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static final class RecordingFrameHandler implements WebSocketFrameHandler {
        private final BlockingQueue<String> textFrames = new LinkedBlockingQueue<>();
        private final AtomicReference<Throwable> disconnected = new AtomicReference<>();

        @Override
        public void onTextFrame(ByteBuf frame) {
            textFrames.add(frame.toString(StandardCharsets.UTF_8));
        }

        @Override
        public void onBinaryFrame(ByteBuf frame) {
        }

        @Override
        public void onDisconnected(Throwable cause) {
            disconnected.compareAndSet(null, cause);
        }

        private String takeText() throws InterruptedException {
            return textFrames.poll(5, TimeUnit.SECONDS);
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
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(8192));
                            ch.pipeline().addLast(new WebSocketServerProtocolHandler("/", null, true, 1024));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<TextWebSocketFrame>() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                    if (evt == WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                                        ctx.writeAndFlush(new TextWebSocketFrame("server-ready"));
                                        return;
                                    }
                                    super.userEventTriggered(ctx, evt);
                                }

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
