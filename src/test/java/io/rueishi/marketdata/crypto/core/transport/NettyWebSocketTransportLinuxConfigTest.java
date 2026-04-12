package io.rueishi.marketdata.crypto.core.transport;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.Test;

/**
 * Configuration tests for Netty transport channel and TLS provider selection.
 *
 * <p>The tests avoid opening live network connections. They verify the static
 * selection hooks that let P2-001 use epoll socket channels when bootstrap
 * supplies an epoll group on Linux, fall back to NIO for test/non-Linux groups,
 * and prefer BoringSSL/OpenSSL when Netty reports it as available.</p>
 */
class NettyWebSocketTransportLinuxConfigTest {

    /**
     * Verifies NIO channel selection for NIO event loop groups.
     */
    @Test
    void selectsNioChannelForNioEventLoopGroup() {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            assertThat(NettyWebSocketTransport.channelClassFor(group)).isEqualTo(NioSocketChannel.class);
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * Verifies epoll channel selection when epoll is available and supplied by bootstrap.
     */
    @Test
    void selectsEpollChannelForEpollEventLoopGroupWhenAvailable() {
        org.junit.jupiter.api.Assumptions.assumeTrue(Epoll.isAvailable());
        EventLoopGroup group = new EpollEventLoopGroup(1);
        try {
            assertThat(NettyWebSocketTransport.channelClassFor(group)).isEqualTo(EpollSocketChannel.class);
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * Verifies the TLS provider hook prefers OpenSSL/BoringSSL when available.
     */
    @Test
    void preferredSslProviderReflectsNettyOpenSslAvailability() {
        SslProvider expected = io.netty.handler.ssl.OpenSsl.isAvailable()
                ? SslProvider.OPENSSL
                : SslProvider.JDK;

        assertThat(NettyWebSocketTransport.preferredSslProvider()).isEqualTo(expected);
    }
}
