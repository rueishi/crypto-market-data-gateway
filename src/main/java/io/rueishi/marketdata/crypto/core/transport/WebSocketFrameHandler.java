package io.rueishi.marketdata.crypto.core.transport;

import io.netty.buffer.ByteBuf;

/**
 * Event-loop callback contract for inbound WebSocket frame delivery.
 *
 * <p>{@link NettyWebSocketTransport} calls this interface after decoding
 * WebSocket frames to direct {@link ByteBuf} payloads. Connector
 * implementations are expected to implement this contract, delegate text and
 * binary frames to their {@code FeedParser}, and trigger recovery when the
 * transport reports an unexpected disconnect.</p>
 */
public interface WebSocketFrameHandler {

    /**
     * Handles one inbound text frame on the connector event-loop thread.
     *
     * <p>The transport owns the buffer lifecycle and releases the frame after
     * this method returns. Implementations must not retain the buffer reference
     * or hand it to another thread.</p>
     *
     * @param frame text frame payload delivered by the WebSocket transport
     */
    void onTextFrame(ByteBuf frame);

    /**
     * Handles one inbound binary frame on the connector event-loop thread.
     *
     * <p>The transport owns the buffer lifecycle and releases the frame after
     * this method returns. Most Phase 1 JSON venues will delegate to the default
     * binary-frame behavior on {@code FeedParser}.</p>
     *
     * @param frame binary frame payload delivered by the WebSocket transport
     */
    void onBinaryFrame(ByteBuf frame);

    /**
     * Handles an unexpected transport disconnection on the connector event-loop thread.
     *
     * <p>Transport implementations must report the cause and avoid performing
     * automatic reconnect policy here; recovery strategy code owns reconnect
     * decisions in later task cards.</p>
     *
     * @param cause disconnection cause reported by the transport, or null if no cause is available
     */
    void onDisconnected(Throwable cause);
}
