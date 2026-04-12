package io.rueishi.marketdata.crypto.core.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Shared Netty WebSocket client transport for connector-owned venue sessions.
 *
 * <p>{@code NettyWebSocketTransport} is the Phase 2 bridge between Netty I/O
 * and the connector-facing {@link WebSocketFrameHandler}. A concrete connector
 * owns one transport, supplies its caller-owned {@link EventLoopGroup},
 * endpoint, TLS context, optional {@link PipelineCustomizer}, frame bounds, and
 * handler, then calls {@link #connect()} before sending authenticated subscribe
 * payloads through {@link #sendText(byte[])}.</p>
 *
 * <p>The class owns the Netty {@link Bootstrap} and channel lifecycle, performs
 * TCP connect, optional TLS, WebSocket upgrade, frame-size enforcement, frame
 * dispatch, ping/pong handling, and channel close. It deliberately does not
 * mutate counters, schedule heartbeat checks, or reconnect automatically; those
 * responsibilities remain with connector, liveness, and recovery strategy code.</p>
 */
public final class NettyWebSocketTransport implements AutoCloseable {
    private static final int HTTP_AGGREGATOR_BYTES = 8_192;

    private final EventLoopGroup eventLoopGroup;
    private final URI uri;
    private final SslContext sslContext;
    private final PipelineCustomizer customizer;
    private final int frameSizeLimit;
    private final int connectTimeoutMs;
    private final WebSocketFrameHandler frameHandler;
    private final Class<? extends SocketChannel> channelClass;
    private final Bootstrap bootstrap;
    private final org.agrona.concurrent.CachedNanoClock cachedNanoClock;

    private volatile Channel channel;

    /**
     * Constructs a transport without opening a network connection.
     *
     * <p>The constructor validates immutable connector transport dependencies
     * and builds a reusable Netty {@link Bootstrap}. Connection state is created
     * by {@link #connect()}, which may be called again after a previous
     * {@link #close()} if the owning recovery strategy decides to reconnect.</p>
     *
     * @param eventLoopGroup caller-owned event loop group for this connector
     * @param uri WebSocket endpoint, using {@code ws} or {@code wss}
     * @param sslContext TLS context for {@code wss} endpoints, or null for plain {@code ws}
     * @param customizer venue hook for optional pipeline handlers
     * @param frameSizeLimit maximum accepted inbound WebSocket payload bytes
     * @param connectTimeoutMs TCP connect timeout in milliseconds
     * @param frameHandler connector callback for inbound frames and disconnect events
     * @throws NullPointerException if required dependencies are null
     * @throws IllegalArgumentException if endpoint, frame size, or timeout inputs are invalid
     */
    public NettyWebSocketTransport(
            EventLoopGroup eventLoopGroup,
            URI uri,
            SslContext sslContext,
            PipelineCustomizer customizer,
            int frameSizeLimit,
            int connectTimeoutMs,
            WebSocketFrameHandler frameHandler) {
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        this.uri = validateUri(uri);
        this.sslContext = sslContext;
        this.customizer = Objects.requireNonNull(customizer, "customizer");
        if (frameSizeLimit <= 0) {
            throw new IllegalArgumentException("frameSizeLimit must be positive");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive");
        }
        if (isSecure(this.uri) && sslContext == null) {
            throw new IllegalArgumentException("sslContext is required for wss endpoints");
        }
        this.frameSizeLimit = frameSizeLimit;
        this.connectTimeoutMs = connectTimeoutMs;
        this.frameHandler = Objects.requireNonNull(frameHandler, "frameHandler");
        this.channelClass = channelClassFor(eventLoopGroup);
        this.cachedNanoClock = null; // no ClockAdvanceHandler; use withClock() overload to enable
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(channelClass)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .handler(new TransportInitializer());
    }

    /**
     * Returns a new transport that wraps this one's configuration and additionally
     * advances the supplied {@link org.agrona.concurrent.CachedNanoClock} at the
     * head of every inbound pipeline frame.
     *
     * <p>Use this factory method instead of the constructor when the connector
     * owns a {@code CachedNanoClock} that must be kept current. The
     * {@link ClockAdvanceHandler} is added first in the channel pipeline so the
     * clock is fresh for all subsequent handlers in the same event-loop
     * iteration.</p>
     *
     * @param clock connector-local cached nanosecond clock to advance per frame
     * @return new transport with the clock-advance handler wired in
     * @throws NullPointerException if {@code clock} is null
     */
    public NettyWebSocketTransport withClock(org.agrona.concurrent.CachedNanoClock clock) {
        Objects.requireNonNull(clock, "clock");
        return new NettyWebSocketTransport(
                eventLoopGroup, uri, sslContext, customizer,
                frameSizeLimit, connectTimeoutMs, frameHandler, clock);
    }

    private NettyWebSocketTransport(
            EventLoopGroup eventLoopGroup,
            URI uri,
            SslContext sslContext,
            PipelineCustomizer customizer,
            int frameSizeLimit,
            int connectTimeoutMs,
            WebSocketFrameHandler frameHandler,
            org.agrona.concurrent.CachedNanoClock cachedNanoClock) {
        this.eventLoopGroup = eventLoopGroup;
        this.uri = uri;
        this.sslContext = sslContext;
        this.customizer = customizer;
        this.frameSizeLimit = frameSizeLimit;
        this.connectTimeoutMs = connectTimeoutMs;
        this.frameHandler = frameHandler;
        this.channelClass = channelClassFor(eventLoopGroup);
        this.cachedNanoClock = cachedNanoClock;
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(channelClass)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .handler(new TransportInitializer());
    }

    /**
     * Connects TCP, completes optional TLS, and waits for WebSocket upgrade.
     *
     * <p>The method blocks the calling thread until Netty reports both channel
     * connection and WebSocket handshake completion, or until either step fails.
     * Reconnect policy is intentionally not implemented here; callers decide
     * whether and when to invoke {@code connect()} again after an {@link IOException}.</p>
     *
     * @throws IOException if TCP connect or WebSocket handshake fails
     */
    public void connect() throws IOException {
        if (isConnected()) {
            return;
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectTimeoutMs);
        HandshakeHandler handshakeHandler = new HandshakeHandler(frameHandler, frameSizeLimit);
        Bootstrap connectBootstrap = bootstrap.clone().handler(new TransportInitializer(handshakeHandler));
        ChannelFuture connectFuture = connectBootstrap.connect(host(), port());
        if (!connectFuture.awaitUninterruptibly(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS)) {
            connectFuture.cancel(true);
            this.channel = null;
            throw new IOException("WebSocket connect timed out for " + safeEndpoint());
        }
        if (!connectFuture.isSuccess()) {
            this.channel = null;
            throw new IOException("WebSocket connect failed for " + safeEndpoint(), connectFuture.cause());
        }

        Channel connectedChannel = connectFuture.channel();
        this.channel = connectedChannel;
        if (!handshakeHandler.awaitHandshake(remainingMillis(deadlineNanos))) {
            Throwable cause = handshakeHandler.handshakeCause();
            connectedChannel.close().syncUninterruptibly();
            this.channel = null;
            if (cause == null) {
                throw new IOException("WebSocket handshake timed out for " + safeEndpoint());
            }
            throw new IOException("WebSocket handshake failed for " + safeEndpoint(), cause);
        }
    }

    /**
     * Sends one pre-encoded UTF-8 JSON payload as a WebSocket text frame.
     *
     * <p>Subscription builders call this after {@link #connect()} succeeds.
     * The payload is wrapped in a Netty buffer and written to the active
     * channel. The write is asynchronous; completion or downstream retry policy
     * is intentionally left to later connector/recovery cards.</p>
     *
     * @param utf8Payload JSON payload bytes owned by the caller
     * @throws NullPointerException if {@code utf8Payload} is null
     * @throws IllegalStateException if the transport is not connected
     */
    public void sendText(byte[] utf8Payload) {
        Objects.requireNonNull(utf8Payload, "utf8Payload");
        Channel activeChannel = requireConnected();
        activeChannel.writeAndFlush(new TextWebSocketFrame(Unpooled.wrappedBuffer(utf8Payload)));
    }

    /**
     * Closes the current WebSocket channel without shutting down the event loop group.
     *
     * <p>The event loop group is owned by bootstrap/connector lifecycle. This
     * method only sends a WebSocket close frame when possible and closes the
     * channel; recovery and graceful shutdown deadline handling are outside
     * P2-001.</p>
     */
    @Override
    public void close() {
        Channel activeChannel = channel;
        if (activeChannel == null) {
            return;
        }
        channel = null;
        if (activeChannel.isActive()) {
            activeChannel.writeAndFlush(new CloseWebSocketFrame()).addListener(future -> activeChannel.close());
            activeChannel.closeFuture().syncUninterruptibly();
        } else {
            activeChannel.close().syncUninterruptibly();
        }
    }

    /**
     * Returns the owning Netty event loop for connector-side scheduling.
     *
     * @return the active channel event loop, or the next group event loop before connection
     */
    public EventLoop eventLoop() {
        Channel activeChannel = channel;
        return activeChannel == null ? eventLoopGroup.next() : activeChannel.eventLoop();
    }

    /**
     * Returns whether the WebSocket channel is currently active.
     *
     * @return true when the channel exists and is active
     */
    public boolean isConnected() {
        Channel activeChannel = channel;
        return activeChannel != null && activeChannel.isActive();
    }

    /**
     * Returns the channel class selected for this transport.
     *
     * <p>Tests use this as a Phase 2 Linux configuration hook. Production code
     * does not need to branch on it after construction.</p>
     *
     * @return Netty socket channel class used by the bootstrap
     */
    public Class<? extends SocketChannel> channelClass() {
        return channelClass;
    }

    /**
     * Builds the production TLS context using BoringSSL/OpenSSL when available.
     *
     * <p>Connector code can call this helper for the default production path,
     * while integration tests may inject a different {@link SslContext} with an
     * in-memory trust manager. The method uses the JDK default trust store.</p>
     *
     * @return client TLS context for {@code wss} endpoints
     * @throws javax.net.ssl.SSLException if Netty cannot build the context
     */
    public static SslContext buildClientSslContext() throws javax.net.ssl.SSLException {
        return SslContextBuilder.forClient()
                .sslProvider(preferredSslProvider())
                .build();
    }

    /**
     * Returns the preferred Netty SSL provider for production transport.
     *
     * @return {@link SslProvider#OPENSSL} when available, otherwise {@link SslProvider#JDK}
     */
    public static SslProvider preferredSslProvider() {
        return io.netty.handler.ssl.OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
    }

    /**
     * Selects the socket channel class for a caller-owned event loop group.
     *
     * <p>On Linux, bootstrap is expected to pass an {@link EpollEventLoopGroup}
     * so the transport can use {@link EpollSocketChannel}. Tests and non-Linux
     * environments can pass {@link NioEventLoopGroup}, which selects
     * {@link NioSocketChannel}.</p>
     *
     * @param eventLoopGroup caller-owned event loop group
     * @return compatible Netty socket channel class
     */
    public static Class<? extends SocketChannel> channelClassFor(EventLoopGroup eventLoopGroup) {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    private Channel requireConnected() {
        Channel activeChannel = channel;
        if (activeChannel == null || !activeChannel.isActive()) {
            throw new IllegalStateException("WebSocket transport is not connected");
        }
        return activeChannel;
    }

    private String host() {
        return uri.getHost();
    }

    private int port() {
        if (uri.getPort() > 0) {
            return uri.getPort();
        }
        return isSecure(uri) ? 443 : 80;
    }

    private String safeEndpoint() {
        return uri.getScheme() + "://" + uri.getHost() + ":" + port() + path();
    }

    private String path() {
        String rawPath = uri.getRawPath();
        String path = rawPath == null || rawPath.isBlank() ? "/" : rawPath;
        return uri.getRawQuery() == null ? path : path + "?" + uri.getRawQuery();
    }

    private static long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    private static URI validateUri(URI uri) {
        Objects.requireNonNull(uri, "uri");
        String scheme = uri.getScheme();
        if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("uri must use ws or wss scheme");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("uri must include a host");
        }
        return uri;
    }

    private static boolean isSecure(URI uri) {
        return "wss".equalsIgnoreCase(uri.getScheme());
    }

    private final class TransportInitializer extends ChannelInitializer<SocketChannel> {
        private final HandshakeHandler handshakeHandler;

        private TransportInitializer() {
            this(null);
        }

        private TransportInitializer(HandshakeHandler handshakeHandler) {
            this.handshakeHandler = handshakeHandler;
        }

        /**
         * Builds the channel pipeline for one connect attempt.
         *
         * @param ch socket channel being initialized by Netty
         */
        @Override
        protected void initChannel(SocketChannel ch) {
            if (cachedNanoClock != null) {
                ch.pipeline().addFirst("clock-advance", new ClockAdvanceHandler(cachedNanoClock));
            }
            if (isSecure(uri)) {
                ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc(), host(), port()));
            }
            ch.pipeline().addLast("http-client", new HttpClientCodec());
            ch.pipeline().addLast("http-aggregator", new HttpObjectAggregator(HTTP_AGGREGATOR_BYTES));
            customizer.customize(ch.pipeline());
            WebSocketClientProtocolConfig config = WebSocketClientProtocolConfig.newBuilder()
                    .webSocketUri(uri)
                    .maxFramePayloadLength(frameSizeLimit)
                    .handleCloseFrames(false)
                    .build();
            ch.pipeline().addLast("websocket-protocol", new WebSocketClientProtocolHandler(config));
            ch.pipeline().addLast("websocket-dispatch", handshakeHandler);
        }
    }

    private static final class HandshakeHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        private final WebSocketFrameHandler frameHandler;
        private final int frameSizeLimit;
        private io.netty.util.concurrent.Promise<Void> handshakePromise;

        private HandshakeHandler(WebSocketFrameHandler frameHandler, int frameSizeLimit) {
            this.frameHandler = frameHandler;
            this.frameSizeLimit = frameSizeLimit;
        }

        /**
         * Captures the channel-scoped promise used to wait for WebSocket upgrade completion.
         *
         * @param ctx channel handler context
         */
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakePromise = ctx.newPromise();
        }

        /**
         * Handles WebSocket handshake events emitted by Netty's protocol handler.
         *
         * @param ctx channel handler context
         * @param evt user event from an upstream handler
         * @throws Exception if superclass event handling fails
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                handshakePromise.trySuccess(null);
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        /**
         * Dispatches inbound WebSocket frames to the connector callback surface.
         *
         * <p>Text and binary frames are passed through as direct Netty payload
         * buffers. Ping frames are answered with pong frames, benign close frames
         * close the channel, and unsupported frame types are ignored. If the
         * connector callback throws, the transport reports the exception and
         * closes the channel to avoid an undefined pipeline state.</p>
         *
         * @param ctx channel handler context
         * @param frame decoded WebSocket frame
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame.content().readableBytes() > frameSizeLimit) {
                IllegalArgumentException failure = new IllegalArgumentException("WebSocket frame exceeds frameSizeLimit");
                frameHandler.onDisconnected(failure);
                ctx.close();
                return;
            }
            try {
                if (frame instanceof TextWebSocketFrame) {
                    frameHandler.onTextFrame(frame.content());
                } else if (frame instanceof BinaryWebSocketFrame) {
                    frameHandler.onBinaryFrame(frame.content());
                } else if (frame instanceof PingWebSocketFrame) {
                    ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                } else if (frame instanceof CloseWebSocketFrame) {
                    ctx.close();
                }
            } catch (RuntimeException ex) {
                frameHandler.onDisconnected(ex);
                ctx.close();
            }
        }

        /**
         * Reports channel loss after a completed handshake.
         *
         * @param ctx channel handler context
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (handshakePromise != null && !handshakePromise.isDone()) {
                handshakePromise.tryFailure(new IOException("Channel closed before WebSocket handshake completed"));
            } else {
                frameHandler.onDisconnected(null);
            }
            super.channelInactive(ctx);
        }

        /**
         * Reports pipeline exceptions to the connector and closes the channel.
         *
         * @param ctx channel handler context
         * @param cause Netty pipeline exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (handshakePromise != null && !handshakePromise.isDone()) {
                handshakePromise.tryFailure(cause);
            }
            frameHandler.onDisconnected(cause);
            ctx.close();
        }

        private boolean awaitHandshake(long timeoutMs) {
            if (timeoutMs <= 0L) {
                return false;
            }
            if (!handshakePromise.awaitUninterruptibly(timeoutMs, TimeUnit.MILLISECONDS)) {
                return false;
            }
            return handshakePromise.isSuccess();
        }

        private Throwable handshakeCause() {
            return handshakePromise.cause();
        }
    }
}
