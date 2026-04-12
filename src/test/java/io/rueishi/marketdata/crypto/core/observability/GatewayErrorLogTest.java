package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link GatewayErrorLog}.
 *
 * <p>The tests verify the Phase 3 hot-path error recording facade around
 * Agrona {@code DistinctErrorLog}. Dependencies are limited to temporary mapped
 * files and Agrona's reader API; scenarios cover repeated exception recording,
 * buffer readability, and invalid pre-existing file sizing.</p>
 */
class GatewayErrorLogTest {

    /**
     * Verifies that repeated failures are recorded through Agrona's distinct
     * error log without throwing, and that the reader sees an aggregated
     * observation count.
     */
    @Test
    void recordsRepeatedErrorsThroughDistinctErrorLog(@TempDir Path tempDir) {
        Path path = tempDir.resolve("gateway-errors");

        try (GatewayErrorLog errorLog = GatewayErrorLog.create(path, 65_536, SystemEpochClock.INSTANCE)) {
            IllegalStateException error = new IllegalStateException("parse failed");
            assertThat(errorLog.record(error)).isTrue();
            assertThat(errorLog.record(error)).isTrue();

            AtomicInteger records = new AtomicInteger();
            AtomicInteger observations = new AtomicInteger();
            int read = ErrorLogReader.read(errorLog.buffer(), (count, first, last, encoded) -> {
                records.incrementAndGet();
                observations.addAndGet(count);
                assertThat(encoded).contains("parse failed");
            });

            assertThat(read).isEqualTo(1);
            assertThat(records).hasValue(1);
            assertThat(observations).hasValue(2);
        }
    }

    /**
     * Verifies that invalid inputs and undersized existing files fail before
     * hot-path callers receive an unusable error-log facade.
     *
     * @throws Exception if the test fixture file cannot be written
     */
    @Test
    void rejectsInvalidInputs(@TempDir Path tempDir) throws Exception {
        assertThatThrownBy(() -> GatewayErrorLog.create(Path.of(""), 64, SystemEpochClock.INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");

        Path undersized = tempDir.resolve("gateway-errors");
        Files.write(undersized, new byte[] {1});
        assertThatThrownBy(() -> GatewayErrorLog.create(undersized, 64, SystemEpochClock.INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("undersized");
    }
}
