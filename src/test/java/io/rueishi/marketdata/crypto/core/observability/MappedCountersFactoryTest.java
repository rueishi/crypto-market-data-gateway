package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link MappedCountersFactory}.
 *
 * <p>The suite verifies the mapped Agrona counter storage used by
 * {@link ObservabilityRuntime}. Tests use JUnit temporary directories instead
 * of OS shared-memory paths, and they cover deterministic metadata/value file
 * creation, abnormal-termination-style remapping where counters are not freed,
 * and fail-fast rejection of undersized remnants.</p>
 */
class MappedCountersFactoryTest {

    /**
     * Verifies that a value written through a mapped counter can be read after
     * the storage is remapped, matching the abnormal JVM termination recovery
     * contract for counter values.
     */
    @Test
    @SuppressWarnings("resource")
    void counterValuesRemainReadableAfterRemap(@TempDir Path tempDir) {
        Path basePath = tempDir.resolve("gateway-counters");
        int counterId;

        try (MappedCountersFactory factory = MappedCountersFactory.create(basePath, 8)) {
            AtomicCounter counter = factory.countersManager().newCounter("gateway_test_counter");
            counterId = counter.id();
            counter.set(42);
        }

        try (MappedCountersFactory remapped = MappedCountersFactory.create(basePath, 8)) {
            assertThat(remapped.countersReader().getCounterValue(counterId)).isEqualTo(42);
            assertThat(remapped.countersReader().getCounterLabel(counterId)).isEqualTo("gateway_test_counter");
        }
    }

    /**
     * Verifies that the configured base path expands into separate Agrona
     * metadata and value files so metrics can read labels and values from the
     * same remapped store.
     */
    @Test
    void createsMetadataAndValueFiles(@TempDir Path tempDir) {
        Path basePath = tempDir.resolve("gateway-counters");

        try (MappedCountersFactory factory = MappedCountersFactory.create(basePath, 4)) {
            assertThat(factory.countersReader()).isInstanceOf(CountersReader.class);
            assertThat(Files.exists(factory.metadataPath())).isTrue();
            assertThat(Files.exists(factory.valuesPath())).isTrue();
        }
    }

    /**
     * Verifies that startup rejects a pre-existing but undersized counter file
     * instead of mapping storage that cannot hold the configured capacity.
     *
     * @throws Exception if the test fixture file cannot be written
     */
    @Test
    void rejectsUndersizedExistingCounterFiles(@TempDir Path tempDir) throws Exception {
        Path basePath = tempDir.resolve("gateway-counters");
        Files.write(basePath.resolveSibling(basePath.getFileName() + ".metadata"), new byte[] {1});

        assertThatThrownBy(() -> MappedCountersFactory.create(basePath, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("undersized");
    }
}
