package io.rueishi.marketdata.crypto.core.parser;

import io.netty.buffer.ByteBuf;

/**
 * Parses inbound WebSocket frames directly from a Netty off-heap {@link ByteBuf}.
 *
 * <p>A venue connector owns one parser instance for its lifetime and calls this
 * interface from the event-loop frame path. Text frames are the primary entry
 * point for JSON venues; binary frames default to an unknown-message counter
 * update unless a venue explicitly supports binary payloads.</p>
 *
 * <p>Implementations must remain stateless with respect to connector session
 * state: they must not store {@link ParseContext} in a field, must not retain
 * the inbound {@code ByteBuf}, and should use the context only as a dependency
 * carrier for the current call. Those rules let later parser implementations
 * process pooled direct buffers without hidden per-message allocation or stale
 * recovery state.</p>
 */
public interface FeedParser {

    /**
     * Parses a UTF-8 text WebSocket frame for the active connector.
     *
     * <p>The caller owns the {@code frame} lifecycle and releases it after this
     * method returns. Implementations should scan the buffer directly, write
     * parse outcomes through dependencies obtained from {@code ctx}, and avoid
     * retaining either argument beyond the call.</p>
     *
     * @param frame inbound text frame payload backed by a Netty {@link ByteBuf}
     * @param ctx per-call parser dependency carrier for counters and later encoding resources
     */
    void onTextFrame(ByteBuf frame, ParseContext ctx);

    /**
     * Handles a binary WebSocket frame for the active connector.
     *
     * <p>Phase 1 JSON venues do not parse binary payloads. The default logic
     * records the frame as an unknown message type through
     * {@link ParseContext#counters()} and then returns without retaining the
     * buffer or changing parser state. Binary venues can override this method in
     * later phases while keeping the same frame ownership contract.</p>
     *
     * @param frame inbound binary frame payload owned and released by the caller
     * @param ctx per-call parser dependency carrier used for the unknown-message counter
     * @throws NullPointerException if {@code ctx} is null or returns null counters
     */
    default void onBinaryFrame(ByteBuf frame, ParseContext ctx) {
        ctx.counters().unknownTypeDrops().increment();
    }
}
