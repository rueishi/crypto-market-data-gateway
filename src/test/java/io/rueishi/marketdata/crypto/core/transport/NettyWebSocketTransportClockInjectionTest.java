package io.rueishi.marketdata.crypto.core.transport;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for transport hooks used by connector-owned time-dependent behavior.
 *
 * <p>P2-001 does not schedule heartbeat liveness checks itself, but it must
 * expose the connector's event loop so later liveness code can schedule tasks
 * with injectable clocks and without wall-clock sleeps in tests. This suite
 * verifies that the event-loop hook is available before connection and runs
 * tasks on the Netty event-loop thread.</p>
 */
class NettyWebSocketTransportClockInjectionTest {

    /**
     * Verifies connector-owned liveness code can schedule work through the exposed event loop.
     *
     * @throws Exception if the event-loop task does not complete
     */
    @Test
    void exposesEventLoopForConnectorOwnedSchedulingHooks() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            NettyWebSocketTransport transport = new NettyWebSocketTransport(
                    group,
                    URI.create("ws://127.0.0.1:65530/"),
                    null,
                    pipeline -> {
                    },
                    1024,
                    100,
                    new NoopFrameHandler());
            EventLoop eventLoop = transport.eventLoop();
            AtomicReference<Thread> executedOn = new AtomicReference<>();

            eventLoop.submit(() -> executedOn.set(Thread.currentThread())).get(5, TimeUnit.SECONDS);

            assertThat(executedOn.get()).isNotNull();
            assertThat(eventLoop.inEventLoop(executedOn.get())).isTrue();
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static final class NoopFrameHandler implements WebSocketFrameHandler {
        @Override
        public void onTextFrame(io.netty.buffer.ByteBuf frame) {
        }

        @Override
        public void onBinaryFrame(io.netty.buffer.ByteBuf frame) {
        }

        @Override
        public void onDisconnected(Throwable cause) {
        }
    }
}
