package io.rueishi.marketdata.crypto.venue.coinbase.l2;

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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only Coinbase L2 WebSocket peer for venue end-to-end integration tests.
 *
 * <p>The server stands in for Coinbase Exchange at the venue-test boundary
 * while production {@link CoinbaseL2Connector}, transport, subscription,
 * parser, publisher, and recovery code stay in the runtime flow. Tests create
 * one server per scenario, pass {@link #uri()} into Coinbase venue config,
 * wait for subscribe/unsubscribe control messages through
 * {@link #awaitSubscribe()} and {@link #awaitUnsubscribe()}, and drive Coinbase
 * L2 fixture frames with the named {@code send...} methods.</p>
 *
 * <p>This helper is deliberately package-private and lives in test sources so
 * core transport tests can continue using their generic local WebSocket server
 * without Coinbase semantics.</p>
 */
final class CoinbaseExchangeL2Server implements AutoCloseable {
    private static final Path FIXTURE_DIR = Path.of("src/test/resources/fixtures/coinbase/l2");
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private final BlockingQueue<String> inboundText = new LinkedBlockingQueue<>();
    private final AtomicReference<Channel> clientChannel = new AtomicReference<>();
    private final Channel serverChannel;
    private final URI uri;

    /**
     * Starts an in-process WebSocket server bound to an ephemeral localhost port.
     *
     * <p>The constructor performs the bind synchronously so {@link #uri()} is
     * immediately usable by connector or bootstrap configuration. Incoming text
     * frames are copied into an in-memory queue for deterministic assertions.</p>
     */
    CoinbaseExchangeL2Server() {
        serverChannel = new ServerBootstrap()
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
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                clientChannel.set(ctx.channel());
                                super.channelActive(ctx);
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
        uri = URI.create("ws://127.0.0.1:" + ((InetSocketAddress) serverChannel.localAddress()).getPort() + "/");
    }

    /**
     * Returns the WebSocket URI for the currently bound simulator endpoint.
     *
     * @return localhost WebSocket URI accepted by Coinbase connector config
     */
    URI uri() {
        return uri;
    }

    /**
     * Waits for and returns the next Coinbase subscribe payload from the client.
     *
     * @return text frame containing {@code "type":"subscribe"}
     * @throws InterruptedException if the waiting test thread is interrupted
     * @throws AssertionError if no matching frame arrives before the bounded timeout
     */
    String awaitSubscribe() throws InterruptedException {
        return awaitTextContaining("\"type\":\"subscribe\"");
    }

    /**
     * Waits for and returns the next Coinbase unsubscribe payload from the client.
     *
     * @return text frame containing {@code "type":"unsubscribe"}
     * @throws InterruptedException if the waiting test thread is interrupted
     * @throws AssertionError if no matching frame arrives before the bounded timeout
     */
    String awaitUnsubscribe() throws InterruptedException {
        return awaitTextContaining("\"type\":\"unsubscribe\"");
    }

    /**
     * Waits for any inbound client payload containing the supplied fragment.
     *
     * <p>Frames that do not match are discarded because tests use this method
     * for ordered control-plane expectations where earlier unexpected frames
     * would already make the scenario invalid.</p>
     *
     * @param fragment text fragment expected in the inbound payload
     * @return matching inbound text frame
     * @throws InterruptedException if the waiting test thread is interrupted
     * @throws AssertionError if the timeout expires before a matching frame arrives
     */
    String awaitTextContaining(String fragment) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            long remainingMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
            String text = inboundText.poll(Math.min(remainingMillis, 100L), TimeUnit.MILLISECONDS);
            if (text != null && text.contains(fragment)) {
                return text;
            }
        }
        throw new AssertionError("Timed out waiting for Coinbase L2 client frame containing " + fragment);
    }

    /** Sends a valid Coinbase subscriptions acknowledgement. */
    void sendValidSubscriptionsAck() {
        sendFixture("subscriptions_ack.json");
    }

    /** Sends a valid acknowledgement with an extra Coinbase channel the parser should ignore. */
    void sendSubscriptionsAckWithExtraChannel() {
        sendFixture("subscriptions_ack_extra_channel.json");
    }

    /** Sends an invalid acknowledgement missing the required level2 channel. */
    void sendBadSubscriptionsAckMissingLevel2() {
        sendFixture("subscriptions_ack_missing_level2.json");
    }

    /** Sends an invalid acknowledgement missing the required heartbeat channel. */
    void sendBadSubscriptionsAckMissingHeartbeat() {
        sendFixture("subscriptions_ack_missing_heartbeat.json");
    }

    /** Sends an invalid acknowledgement for the wrong product id. */
    void sendBadSubscriptionsAckWrongProduct() {
        sendFixture("subscriptions_ack_wrong_product.json");
    }

    /** Sends the standard Coinbase L2 snapshot fixture. */
    void sendSnapshot() {
        sendFixture("snapshot.json");
    }

    /** Sends a valid snapshot with no top-level time field. */
    void sendSnapshotWithoutTime() {
        sendFixture("snapshot_without_time.json");
    }

    /** Sends a valid snapshot whose epoch-zero timestamp should encode as the sentinel. */
    void sendSnapshotEpochZeroTime() {
        sendFixture("snapshot_epoch_zero_time.json");
    }

    /** Sends a wrong-product snapshot that must be counted and dropped. */
    void sendSnapshotWrongProduct() {
        sendFixture("snapshot_wrong_product.json");
    }

    /** Sends a one-row buy upsert update fixture. */
    void sendL2UpdateBuyUpsert() {
        sendFixture("l2update_buy_upsert.json");
    }

    /** Sends a one-row sell delete update fixture. */
    void sendL2UpdateSellDelete() {
        sendFixture("l2update_sell_delete.json");
    }

    /** Sends a multi-change update fixture used to verify entry-count patching. */
    void sendL2UpdateMultiChange() {
        sendFixture("l2update_multi_change.json");
    }

    /** Sends a pre-snapshot update technical anomaly fixture. */
    void sendL2UpdateBeforeSnapshot() {
        sendFixture("l2update_before_snapshot.json");
    }

    /** Sends a wrong-product update that must be counted and dropped. */
    void sendL2UpdateWrongProduct() {
        sendFixture("l2update_wrong_product.json");
    }

    /** Sends a matching-product heartbeat fixture. */
    void sendHeartbeat() {
        sendFixture("heartbeat.json");
    }

    /** Sends a wrong-product heartbeat that must be counted and dropped. */
    void sendHeartbeatWrongProduct() {
        sendFixture("heartbeat_wrong_product.json");
    }

    /** Sends an unknown message type fixture. */
    void sendUnknownType() {
        sendFixture("unknown_type.json");
    }

    /** Sends a malformed JSON/decimal fixture. */
    void sendMalformed() {
        sendFixture("malformed.json");
    }

    /** Sends a bad-decimal fixture that must be rejected as malformed. */
    void sendBadDecimal() {
        sendFixture("bad_decimal.json");
    }

    /** Sends the recovery snapshot fixture. */
    void sendRecoverySnapshot() {
        sendFixture("recovery_snapshot.json");
    }

    /** Sends the recovery l2update fixture. */
    void sendRecoveryL2Update() {
        sendFixture("recovery_l2update.json");
    }

    /**
     * Closes only the currently connected client channel.
     *
     * <p>The listening socket remains open so tests can force a disconnect and
     * then allow production reconnect logic to establish a replacement client
     * connection to the same simulator.</p>
     */
    void closeClientConnection() {
        Channel connectedClient = clientChannel.getAndSet(null);
        if (connectedClient != null) {
            connectedClient.close().syncUninterruptibly();
        }
    }

    /**
     * Loads a fixture and sends it as a Coinbase WebSocket text frame.
     *
     * @param name fixture filename under {@code src/test/resources/fixtures/coinbase/l2}
     * @throws IllegalStateException if the fixture cannot be read
     * @throws AssertionError if no active client channel is available before the timeout
     */
    private void sendFixture(String name) {
        Path fixture = FIXTURE_DIR.resolve(name);
        try {
            send(Files.readString(fixture, StandardCharsets.UTF_8));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read Coinbase L2 fixture " + fixture, ex);
        }
    }

    private void send(String json) {
        Channel channel = awaitActiveClient();
        channel.writeAndFlush(new TextWebSocketFrame(json)).syncUninterruptibly();
    }

    private Channel awaitActiveClient() {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            Channel channel = clientChannel.get();
            if (channel != null && channel.isActive()) {
                return channel;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("Timed out waiting for active Coinbase L2 client channel");
    }

    /**
     * Closes the active client, listening socket, and Netty event-loop groups.
     */
    @Override
    public void close() {
        closeClientConnection();
        serverChannel.close().syncUninterruptibly();
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
