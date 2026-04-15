package io.rueishi.marketdata.crypto.venue.coinbase.l3;

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
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only Coinbase L3 full-feed WebSocket peer used by the Phase 4 end-to-end suite.
 *
 * <p>The server mirrors the L2 {@code CoinbaseExchangeL2Server} helper but speaks the
 * Coinbase full-channel JSON dialect: {@code subscriptions}, {@code received},
 * {@code open}, {@code done}, {@code activate}, {@code change}, {@code match},
 * {@code heartbeat}, {@code error}, {@code malformed}, and {@code unknown} frames. It is
 * created once per E2E test scenario and drives the production {@link CoinbaseL3Connector}
 * through its public WebSocket endpoint. The listening socket stays bound across
 * {@link #closeClientConnection() client disconnects} so recovery tests can exercise the
 * reconnect-and-resubscribe path.</p>
 *
 * <p>A test typically instantiates one server, passes {@link #uri()} into venue
 * configuration, awaits the inbound subscribe payload via {@link #awaitSubscribe()},
 * then calls the {@code send...} helpers to drive the protocol state machine. The
 * production connector parses the injected frames exactly as it would parse real Coinbase
 * traffic; no test hooks are wired into the production code.</p>
 *
 * <p>This helper is package-private and intentionally lives in test sources so no
 * venue-agnostic core code ever depends on Coinbase JSON semantics.</p>
 */
final class CoinbaseExchangeL3Server implements AutoCloseable {
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private final BlockingQueue<String> inboundText = new LinkedBlockingQueue<>();
    private final AtomicReference<Channel> clientChannel = new AtomicReference<>();
    private final Channel serverChannel;
    private final URI uri;

    /**
     * Starts an in-process Coinbase L3 WebSocket simulator bound to an ephemeral localhost port.
     *
     * <p>The constructor binds synchronously so {@link #uri()} is immediately usable as the
     * {@code endpoint} entry of Coinbase venue config. Inbound text frames are captured into
     * an unbounded queue for deterministic subscribe/unsubscribe assertions.</p>
     */
    CoinbaseExchangeL3Server() {
        serverChannel = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(16384));
                        ch.pipeline().addLast(new WebSocketServerProtocolHandler("/", null, true, 16384));
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
     * Returns the WebSocket URI that test configuration passes into Coinbase venue config.
     *
     * @return localhost WebSocket URI such as {@code ws://127.0.0.1:<port>/}
     */
    URI uri() {
        return uri;
    }

    /**
     * Waits for the next inbound Coinbase subscribe payload and returns its JSON text.
     *
     * @return subscribe payload text containing {@code "type":"subscribe"}
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws AssertionError if no matching frame arrives within {@link #WAIT_TIMEOUT}
     */
    String awaitSubscribe() throws InterruptedException {
        return awaitTextContaining("\"type\":\"subscribe\"");
    }

    /**
     * Waits for the next inbound Coinbase unsubscribe payload and returns its JSON text.
     *
     * @return unsubscribe payload text containing {@code "type":"unsubscribe"}
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws AssertionError if no matching frame arrives within {@link #WAIT_TIMEOUT}
     */
    String awaitUnsubscribe() throws InterruptedException {
        return awaitTextContaining("\"type\":\"unsubscribe\"");
    }

    private String awaitTextContaining(String fragment) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            long remainingMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
            String text = inboundText.poll(Math.min(remainingMillis, 100L), TimeUnit.MILLISECONDS);
            if (text != null && text.contains(fragment)) {
                return text;
            }
        }
        throw new AssertionError("Timed out waiting for Coinbase L3 client frame containing " + fragment);
    }

    /**
     * Sends the standard Coinbase L3 subscriptions acknowledgement containing {@code full} and
     * {@code heartbeat} for a single product.
     *
     * @param productId Coinbase product identifier such as {@code BTC-USD}
     */
    void sendSubscriptionsAck(String productId) {
        send("{\"type\":\"subscriptions\",\"channels\":["
                + "{\"name\":\"full\",\"product_ids\":[\"" + productId + "\"]},"
                + "{\"name\":\"heartbeat\",\"product_ids\":[\"" + productId + "\"]}"
                + "]}");
    }

    /** Sends a subscriptions ack that omits the required {@code full} channel. */
    void sendSubscriptionsAckMissingFull(String productId) {
        send("{\"type\":\"subscriptions\",\"channels\":["
                + "{\"name\":\"heartbeat\",\"product_ids\":[\"" + productId + "\"]}"
                + "]}");
    }

    /**
     * Sends a Coinbase {@code received} lifecycle frame.
     *
     * @param productId Coinbase product id
     * @param orderId order UUID
     * @param side {@code buy} or {@code sell}
     * @param orderType {@code limit}, {@code market}, or {@code stop}
     * @param price quoted price string; pass the empty string for absent (market) orders
     * @param size quoted size string; pass the empty string for absent (market) orders
     * @param sequence Coinbase per-product sequence
     */
    void sendReceived(String productId, String orderId, String side, String orderType,
                      String price, String size, long sequence) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("{\"type\":\"received\",\"time\":\"2023-02-09T20:33:09.123456Z\"")
                .append(",\"product_id\":\"").append(productId).append("\"")
                .append(",\"sequence\":").append(sequence)
                .append(",\"order_id\":\"").append(orderId).append("\"")
                .append(",\"side\":\"").append(side).append("\"")
                .append(",\"order_type\":\"").append(orderType).append("\"");
        if (!price.isEmpty()) {
            sb.append(",\"price\":\"").append(price).append("\"");
        }
        if (!size.isEmpty()) {
            sb.append(",\"size\":\"").append(size).append("\"");
        }
        sb.append("}");
        send(sb.toString());
    }

    /** Sends a Coinbase {@code open} frame indicating a resting limit order. */
    void sendOpen(String productId, String orderId, String side, String price,
                  String remainingSize, long sequence) {
        send("{\"type\":\"open\",\"time\":\"2023-02-09T20:33:09.456789Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"order_id\":\"" + orderId + "\""
                + ",\"side\":\"" + side + "\""
                + ",\"price\":\"" + price + "\""
                + ",\"remaining_size\":\"" + remainingSize + "\"}");
    }

    /**
     * Sends a Coinbase {@code done} frame.
     *
     * @param reason {@code filled} or {@code canceled}
     */
    void sendDone(String productId, String orderId, String side, String reason,
                  String price, String remainingSize, long sequence) {
        send("{\"type\":\"done\",\"time\":\"2023-02-09T20:33:09.800000Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"order_id\":\"" + orderId + "\""
                + ",\"side\":\"" + side + "\""
                + ",\"reason\":\"" + reason + "\""
                + ",\"price\":\"" + price + "\""
                + ",\"remaining_size\":\"" + remainingSize + "\"}");
    }

    /** Sends a Coinbase {@code activate} frame for a stop-order triggered event. */
    void sendActivate(String productId, String orderId, String side, String stopPrice, long sequence) {
        send("{\"type\":\"activate\",\"time\":\"2023-02-09T20:33:10.000000Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"order_id\":\"" + orderId + "\""
                + ",\"side\":\"" + side + "\""
                + ",\"stop_type\":\"entry\""
                + ",\"stop_price\":\"" + stopPrice + "\"}");
    }

    /** Sends a Coinbase {@code change} frame with both new and old sizes. */
    void sendChange(String productId, String orderId, String side, String price,
                    String newSize, String oldSize, long sequence) {
        send("{\"type\":\"change\",\"time\":\"2023-02-09T20:33:10.100000Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"order_id\":\"" + orderId + "\""
                + ",\"side\":\"" + side + "\""
                + ",\"price\":\"" + price + "\""
                + ",\"new_size\":\"" + newSize + "\""
                + ",\"old_size\":\"" + oldSize + "\"}");
    }

    /** Sends a Coinbase {@code match} trade frame. */
    void sendMatch(String productId, String makerOrderId, String takerOrderId, String side,
                   String price, String size, long sequence) {
        send("{\"type\":\"match\",\"trade_id\":10,\"time\":\"2023-02-09T20:33:10.200000Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"maker_order_id\":\"" + makerOrderId + "\""
                + ",\"taker_order_id\":\"" + takerOrderId + "\""
                + ",\"side\":\"" + side + "\""
                + ",\"price\":\"" + price + "\""
                + ",\"size\":\"" + size + "\"}");
    }

    /** Sends a Coinbase heartbeat frame matching the configured product id. */
    void sendHeartbeat(String productId) {
        send("{\"type\":\"heartbeat\",\"product_id\":\"" + productId + "\"}");
    }

    /** Sends a Coinbase protocol-level error frame so the connector escalates recovery. */
    void sendError() {
        send("{\"type\":\"error\",\"message\":\"simulator error\"}");
    }

    /** Sends a wholly unknown frame type the parser must count and drop. */
    void sendUnknownType() {
        send("{\"type\":\"wibble\",\"product_id\":\"BTC-USD\"}");
    }

    /** Sends a malformed JSON payload that cannot be parsed. */
    void sendMalformed() {
        send("{\"type\":\"received\",\"bad_json\":");
    }

    /** Sends a {@code received} frame with a non-numeric price field for bad-decimal tests. */
    void sendBadDecimalPrice(String productId, long sequence) {
        send("{\"type\":\"received\",\"time\":\"2023-02-09T20:33:09.123456Z\""
                + ",\"product_id\":\"" + productId + "\""
                + ",\"sequence\":" + sequence
                + ",\"order_id\":\"d50ec984-77a8-460a-b958-66f114b0de9b\""
                + ",\"side\":\"buy\",\"order_type\":\"limit\""
                + ",\"price\":\"not_a_number\",\"size\":\"1.0\"}");
    }

    /** Sends an {@code open} frame with a wrong {@code product_id} that must be counted as a drop. */
    void sendOpenWrongProduct(String wrongProductId, long sequence) {
        sendOpen(wrongProductId, "d50ec984-77a8-460a-b958-66f114b0de9b", "buy", "333.98", "1.0", sequence);
    }

    /**
     * Closes the currently attached client channel while keeping the listening socket bound.
     *
     * <p>Used by recovery tests to simulate a channel drop. The production connector's
     * recovery strategy must reconnect to the same endpoint and drive a fresh subscribe.</p>
     */
    void closeClientConnection() {
        Channel connectedClient = clientChannel.getAndSet(null);
        if (connectedClient != null) {
            connectedClient.close().syncUninterruptibly();
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
        throw new AssertionError("Timed out waiting for active Coinbase L3 client channel");
    }

    /**
     * Closes the active client channel, the listening socket, and the Netty event-loop groups.
     */
    @Override
    public void close() {
        closeClientConnection();
        serverChannel.close().syncUninterruptibly();
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
