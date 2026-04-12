package io.rueishi.marketdata.crypto.core.transport;

import io.netty.channel.ChannelPipeline;

/**
 * Venue hook for adding optional handlers during Netty pipeline construction.
 *
 * <p>{@link NettyWebSocketTransport} invokes this hook once while building a
 * connector-owned Netty pipeline. Connector factories use it for optional
 * venue-specific handlers such as decompression; most venues pass a no-op
 * implementation that leaves the shared HTTP/WebSocket handlers unchanged.</p>
 */
@FunctionalInterface
public interface PipelineCustomizer {

    /**
     * Customizes a Netty channel pipeline during transport setup.
     *
     * <p>The method is called once during channel initialization, outside the
     * per-message parser hot path. Implementations should add only venue-specific
     * handlers and must not start transport or reconnect behavior themselves.</p>
     *
     * @param pipeline Netty channel pipeline being configured for a connector transport
     */
    void customize(ChannelPipeline pipeline);
}
