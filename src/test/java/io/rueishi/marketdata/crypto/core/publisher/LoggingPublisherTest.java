package io.rueishi.marketdata.crypto.core.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.LoggingPublisherConfig;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the asynchronous {@link LoggingPublisher} boundary.
 *
 * <p>The tests verify the publisher contract for the Log4j 2 logging adapter:
 * normal publish calls are accepted, copied into async log records, and update
 * publisher-stage latency counters, while reset publication remains a
 * control-path operation that does not affect publisher latency metrics.</p>
 */
class LoggingPublisherTest {

    /**
     * Verifies normal publish accepts the buffer and records publisher-stage latency.
     */
    @Test
    void publishUpdatesMessageAndPublisherLatencyCounters(@TempDir Path tempDir) throws Exception {
        Path outputPath = tempDir.resolve("messages.log");
        Files.deleteIfExists(outputPath);
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[] {1, 2, 3});
        AtomicLong now = new AtomicLong(10L);

        try (LoggingPublisher publisher = new LoggingPublisher(config(outputPath))) {
            assertThat(publisher.publish(buffer, 0, 3, counters, () -> now.getAndAdd(5L))).isTrue();
        }

        assertThat(counters.messagesPublished().get()).isEqualTo(1);
        assertThat(counters.publisherLatencyLastNanos().get()).isEqualTo(5L);
        assertThat(counters.publisherLatencyMinNanos().get()).isEqualTo(5L);
        assertThat(counters.publisherLatencyMaxNanos().get()).isEqualTo(5L);
        assertThat(Files.readAllLines(outputPath)).containsExactly("sbe_base64=AQID");
    }

    /**
     * Verifies an existing log file is appended to and the newest chunk is the latest published payload.
     */
    @Test
    void publishAppendsLatestChunkAfterExistingLogContent(@TempDir Path tempDir) throws Exception {
        Path outputPath = tempDir.resolve("messages.log");
        Files.writeString(outputPath, "sbe_base64=older-message" + System.lineSeparator());
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[] {4, 5, 6});

        try (LoggingPublisher publisher = new LoggingPublisher(config(outputPath))) {
            assertThat(publisher.publish(buffer, 0, 3, null, null)).isTrue();
        }

        assertThat(Files.readAllLines(outputPath))
                .containsExactly("sbe_base64=older-message", "sbe_base64=BAUG");
    }

    /**
     * Verifies reset publication does not record publisher-stage latency.
     */
    @Test
    void publishResetDoesNotUpdatePublisherLatencyCounters(@TempDir Path tempDir) {
        LoggingPublisher publisher = new LoggingPublisher(config(tempDir.resolve("messages.log")));
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();

        publisher.publishReset(1001, (byte) 1, (byte) 2, (byte) 1, () -> 100L);
        publisher.close();

        assertThat(counters.publisherLatencyLastNanos().get()).isZero();
        assertThat(counters.publisherLatencyMinNanos().get()).isZero();
        assertThat(counters.publisherLatencyMaxNanos().get()).isZero();
    }

    private static LoggingPublisherConfig config(Path outputPath) {
        LoggingPublisherConfig config = new LoggingPublisherConfig();
        config.outputPath = outputPath.toString();
        config.rollSizeMb = 1;
        return config;
    }
}
