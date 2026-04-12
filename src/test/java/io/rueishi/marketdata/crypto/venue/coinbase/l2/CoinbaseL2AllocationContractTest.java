package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rueishi.marketdata.crypto.core.parser.ByteBufScanner;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Allocation and context-retention contract tests for the Coinbase L2 parser path.
 *
 * <p>The tests guard the hot parser helpers used by {@link CoinbaseL2FeedParser}
 * and {@link ByteBufScanner}. They combine a structural reflection check for
 * forbidden retained runtime contexts with a best-effort allocation recorder
 * check for scanner decimal/timestamp parsing. When the running JVM does not
 * expose per-thread allocation accounting, the allocation portion skips
 * cleanly and the structural contract still runs.</p>
 */
class CoinbaseL2AllocationContractTest {

    /**
     * Verifies the Coinbase parser stores only stable instrument identity and
     * never retains call-scoped parser, snapshot, or recovery context objects.
     */
    @Test
    void coinbaseParserDoesNotRetainRuntimeContextsOrStringSymbols() {
        assertThat(CoinbaseL2FeedParser.class.getDeclaredFields())
                .filteredOn(field -> !field.isSynthetic())
                .allSatisfy(CoinbaseL2AllocationContractTest::assertParserFieldIsAllowed);
    }

    /**
     * Verifies scanner decimal and timestamp helpers stay allocation-free on
     * the explicit-range path used by venue parsers, when JVM accounting is
     * available.
     */
    @Test
    void byteBufScannerRangeParsingDoesNotAllocateWhenRecorderIsAvailable() {
        com.sun.management.ThreadMXBean threadBean = allocationBean();
        assumeTrue(threadBean != null && threadBean.isThreadAllocatedMemorySupported(),
                "Thread allocation accounting is not available on this JVM");
        if (!threadBean.isThreadAllocatedMemoryEnabled()) {
            threadBean.setThreadAllocatedMemoryEnabled(true);
        }

        ByteBuf decimal = Unpooled.wrappedBuffer("21921.73000000".getBytes(StandardCharsets.US_ASCII));
        ByteBuf timestamp = Unpooled.wrappedBuffer("2023-02-09T20:33:10.123456789Z".getBytes(StandardCharsets.US_ASCII));
        long[] result = new long[2];
        for (int i = 0; i < 10_000; i++) {
            ByteBufScanner.parseDecimal(decimal, 0, decimal.writerIndex(), result);
            ByteBufScanner.parseRfc3339ToEpochNanos(timestamp, 0, timestamp.writerIndex());
        }

        long threadId = Thread.currentThread().getId();
        long before = threadBean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < 100_000; i++) {
            ByteBufScanner.parseDecimal(decimal, 0, decimal.writerIndex(), result);
            ByteBufScanner.parseRfc3339ToEpochNanos(timestamp, 0, timestamp.writerIndex());
        }
        long allocated = threadBean.getThreadAllocatedBytes(threadId) - before;

        assertThat(allocated).isLessThan(1_024L);
    }

    private static void assertParserFieldIsAllowed(Field field) {
        Class<?> type = field.getType();
        assertThat(type)
                .as(field.getName() + " must not retain a call-scoped context")
                .isNotIn(ParseContext.class, SnapshotContext.class, RecoveryContext.class);
        assertThat(type)
                .as(field.getName() + " must not store product symbols as String values")
                .isNotEqualTo(String.class);
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        if (threadBean instanceof com.sun.management.ThreadMXBean allocationBean) {
            return allocationBean;
        }
        return null;
    }
}
