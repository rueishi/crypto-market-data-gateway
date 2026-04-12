package io.rueishi.marketdata.crypto.core.connector;

import io.netty.buffer.ByteBuf;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test parser that records frame delegation from {@link AbstractConnector}.
 */
final class RecordingParser implements FeedParser {
    final AtomicInteger textCalls = new AtomicInteger();
    final AtomicInteger binaryCalls = new AtomicInteger();
    ParseContext lastContext;

    @Override
    public void onTextFrame(ByteBuf frame, ParseContext ctx) {
        textCalls.incrementAndGet();
        lastContext = ctx;
    }

    @Override
    public void onBinaryFrame(ByteBuf frame, ParseContext ctx) {
        binaryCalls.incrementAndGet();
        lastContext = ctx;
    }
}
