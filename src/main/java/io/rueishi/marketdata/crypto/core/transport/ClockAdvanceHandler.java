package io.rueishi.marketdata.crypto.core.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Objects;
import org.agrona.concurrent.CachedNanoClock;

/**
 * Netty pipeline handler that advances the connector's {@link CachedNanoClock} on every inbound frame.
 *
 * <p>{@code CachedNanoClock} does NOT self-update. Without an explicit
 * {@link CachedNanoClock#advance()} call before each {@code nanoTime()} read,
 * the clock returns the value from the moment it was constructed — making every
 * {@code ingressTimestamp}, liveness seed, and publisher latency measurement
 * wrong for the lifetime of the process.</p>
 *
 * <p>This handler is added at the head of the connector's Netty pipeline so
 * the clock is advanced once per Netty event-loop iteration before any other
 * handler sees the frame. All subsequent {@code nanoTime()} calls within the
 * same iteration — parse context ingress capture, liveness counter updates,
 * publisher latency start — therefore read a fresh value.</p>
 *
 * <p>This handler is NOT {@code @Sharable}: one instance must be created per
 * connector pipeline, matching the one-clock-per-connector ownership model.</p>
 */
public final class ClockAdvanceHandler extends ChannelInboundHandlerAdapter {

    private final CachedNanoClock clock;

    /**
     * Creates a handler that advances the supplied clock on every inbound frame.
     *
     * @param clock the connector-local cached nanosecond clock to advance
     * @throws NullPointerException if {@code clock} is null
     */
    public ClockAdvanceHandler(CachedNanoClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Advances the cached clock, then fires the event to the next handler.
     *
     * @param ctx channel handler context
     * @param msg inbound message passed unchanged to the next handler
     * @throws Exception if the next handler throws
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        clock.update(System.nanoTime());  // refresh cached nanoTime() for this event-loop iteration
        ctx.fireChannelRead(msg);  // pass frame to the rest of the pipeline unmodified
    }
}
