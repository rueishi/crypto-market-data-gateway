package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;

/**
 * Reuse and publisher-result tests for {@link SbeEncoder}.
 *
 * <p>The tests verify that the direct buffer is reused across messages, that
 * {@link SbeEncoder#reuseCount()} tracks finalized messages, and that reuse
 * count increments even when a publisher reports backpressure.</p>
 */
class SbeEncoderReuseTest {
    private static final int WARMUP_MESSAGES = 10_000;
    private static final int HOT_PATH_MESSAGES = 1_000_000;

    /**
     * Verifies repeated messages reuse one buffer while returning the publisher result unchanged.
     */
    @Test
    void reuseCountTracksEndMessageCallsRegardlessOfPublishOutcome() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 2, 8);
        int capacity = encoder.capacity();

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 10L);
        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isTrue();

        publisher.nextPublishResult(false);
        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 2, 2, 2, -1L, 11L);
        encoder.writeLevel((byte) 1, (byte) 1, 10L, (byte) 1, 20L, (byte) 2);
        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isFalse();

        assertThat(encoder.reuseCount()).isEqualTo(2);
        assertThat(encoder.capacity()).isEqualTo(capacity);
    }

    /**
     * Verifies the Phase 1 hot path does not allocate heap per encoded and published message.
     */
    @Test
    void encodePublishLoopDoesNotAllocateHeapAcrossOneMillionMessages() {
        com.sun.management.ThreadMXBean threadBean = threadAllocatedBytesBean();
        InMemoryPublisher publisher = new InMemoryPublisher(8, 256);
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);
        int capacity = encoder.capacity();

        for (int pass = 0; pass < 3; pass++) {
            runEncodePublishLoop(encoder, publisher, WARMUP_MESSAGES);
        }
        long reuseCountBeforeMeasuredLoop = encoder.reuseCount();
        long threadId = Thread.currentThread().getId();
        long allocatedBefore = threadBean.getThreadAllocatedBytes(threadId);

        runEncodePublishLoop(encoder, publisher, HOT_PATH_MESSAGES);

        long allocatedAfter = threadBean.getThreadAllocatedBytes(threadId);
        long allocatedBytes = allocatedAfter - allocatedBefore;
        assertThat(allocatedBytes)
                .describedAs("fixed JVM measurement noise only, not per-message heap allocation")
                .isLessThan(4_096L);
        assertThat(encoder.reuseCount() - reuseCountBeforeMeasuredLoop).isEqualTo(HOT_PATH_MESSAGES);
        assertThat(encoder.capacity()).isEqualTo(capacity);
        assertThat(publisher.totalCount()).isEqualTo((3 * WARMUP_MESSAGES) + HOT_PATH_MESSAGES);
        assertThat(publisher.allMessages()).hasSize(8);
    }

    private static void runEncodePublishLoop(SbeEncoder encoder, InMemoryPublisher publisher, int messageCount) {
        for (int i = 1; i <= messageCount; i++) {
            encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, i, i, i, -1L, i);
            encoder.writeLevel((byte) 1, (byte) 1, 10L, (byte) 1, 20L, (byte) 2);
            encoder.endMessage(publisher, null, null);
        }
    }

    private static com.sun.management.ThreadMXBean threadAllocatedBytesBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        assertThat(bean).isInstanceOf(com.sun.management.ThreadMXBean.class);
        com.sun.management.ThreadMXBean threadBean = (com.sun.management.ThreadMXBean) bean;
        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();
        if (!threadBean.isThreadAllocatedMemoryEnabled()) {
            threadBean.setThreadAllocatedMemoryEnabled(true);
        }
        return threadBean;
    }
}
