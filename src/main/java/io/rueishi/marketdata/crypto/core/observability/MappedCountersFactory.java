package io.rueishi.marketdata.crypto.core.observability;

import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Factory for Agrona counters backed by deterministic memory-mapped files.
 *
 * <p>{@code MappedCountersFactory} is used by {@link ObservabilityRuntime}
 * during bootstrap, before connectors and parsers are initialized. It converts
 * the configured counter path into separate metadata and value files, maps both
 * files, and returns a {@link CountersManager} whose storage can also be read
 * through the inherited {@link CountersReader} API used by metrics export.</p>
 *
 * <p>The factory owns the mapped byte buffers, not the individual
 * {@code AtomicCounter} objects allocated through the manager. Runtime shutdown
 * therefore closes {@link GatewayCounters} first to release counter ids, then
 * closes this factory to force and unmap the storage.</p>
 */
public final class MappedCountersFactory implements AutoCloseable {
    /** Default capacity used by the gateway until a later config card makes it tunable. */
    public static final int DEFAULT_MAX_COUNTERS = 4_096;

    private final Path basePath;
    private final Path metadataPath;
    private final Path valuesPath;
    private final MappedByteBuffer metadataMappedBuffer;
    private final MappedByteBuffer valuesMappedBuffer;
    private final CountersManager countersManager;
    private boolean closed;

    /**
     * Maps the default gateway counter capacity at the supplied base path.
     *
     * @param basePath configured shared-memory path; {@code .metadata} and {@code .values} suffixes are derived from it
     * @return mapped counter factory with a ready {@link CountersManager}
     * @throws NullPointerException if {@code basePath} is null
     * @throws IllegalArgumentException if {@code basePath} is blank
     * @throws UncheckedIOException if file creation, sizing, or mapping fails
     */
    public static MappedCountersFactory create(Path basePath) {
        return create(basePath, DEFAULT_MAX_COUNTERS);
    }

    /**
     * Maps counter metadata and value files for the requested counter capacity.
     *
     * <p>Tests use the capacity overload to keep temporary files small. Existing
     * files must already be at least the expected size; this prevents startup
     * from silently accepting undersized remnants from a bad deployment while
     * still allowing correctly sized files to be remapped after abnormal JVM
     * termination.</p>
     *
     * @param basePath configured shared-memory path used as the file-name base
     * @param maxCounters maximum number of counters the manager can allocate
     * @return mapped counter factory with a ready {@link CountersManager}
     * @throws NullPointerException if {@code basePath} is null
     * @throws IllegalArgumentException if {@code basePath} is blank or {@code maxCounters} is not positive
     * @throws UncheckedIOException if file creation, sizing, or mapping fails
     */
    public static MappedCountersFactory create(Path basePath, int maxCounters) {
        Objects.requireNonNull(basePath, "basePath");
        if (basePath.toString().isBlank()) {
            throw new IllegalArgumentException("basePath must not be blank");
        }
        if (maxCounters <= 0) {
            throw new IllegalArgumentException("maxCounters must be positive");
        }

        Path metadataPath = Path.of(basePath + ".metadata");
        Path valuesPath = Path.of(basePath + ".values");
        int metadataLength = CountersReader.METADATA_LENGTH * maxCounters;
        int valuesLength = CountersReader.COUNTER_LENGTH * maxCounters;
        MappedByteBuffer metadata = mapExistingOrNew(metadataPath, metadataLength, "counter metadata");
        MappedByteBuffer values = mapExistingOrNew(valuesPath, valuesLength, "counter values");
        return new MappedCountersFactory(basePath, metadataPath, valuesPath, metadata, values);
    }

    private MappedCountersFactory(
            Path basePath,
            Path metadataPath,
            Path valuesPath,
            MappedByteBuffer metadataMappedBuffer,
            MappedByteBuffer valuesMappedBuffer) {
        this.basePath = basePath;
        this.metadataPath = metadataPath;
        this.valuesPath = valuesPath;
        this.metadataMappedBuffer = metadataMappedBuffer;
        this.valuesMappedBuffer = valuesMappedBuffer;
        this.countersManager = new CountersManager(
                new UnsafeBuffer(metadataMappedBuffer),
                new UnsafeBuffer(valuesMappedBuffer));
    }

    /**
     * Returns the shared Agrona counter manager used to allocate gateway and instrument counters.
     *
     * @return mapped counter manager
     * @throws IllegalStateException if the factory has already been closed
     */
    public CountersManager countersManager() {
        ensureOpen();
        return countersManager;
    }

    /**
     * Returns a generic counter reader over the same mapped buffers.
     *
     * @return counter reader used by metrics discovery and tests
     * @throws IllegalStateException if the factory has already been closed
     */
    public CountersReader countersReader() {
        ensureOpen();
        return countersManager;
    }

    /**
     * Returns the configured base path used to derive metadata and value file names.
     *
     * @return configured base path
     */
    public Path basePath() {
        return basePath;
    }

    /**
     * Returns the mapped metadata file path derived from the configured base path.
     *
     * @return metadata file path
     */
    public Path metadataPath() {
        return metadataPath;
    }

    /**
     * Returns the mapped values file path derived from the configured base path.
     *
     * @return values file path
     */
    public Path valuesPath() {
        return valuesPath;
    }

    /**
     * Forces dirty counter pages to disk and unmaps the underlying buffers.
     *
     * <p>This method is idempotent. It does not close any {@code AtomicCounter}
     * objects allocated by {@link GatewayCounters}; those counters are released
     * by the facade before the mapped storage is unmapped.</p>
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        valuesMappedBuffer.force();
        metadataMappedBuffer.force();
        IoUtil.unmap(valuesMappedBuffer);
        IoUtil.unmap(metadataMappedBuffer);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("mapped counters are closed");
        }
    }

    private static MappedByteBuffer mapExistingOrNew(Path path, int expectedLength, String description) {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            if (Files.exists(path)) {
                long size = Files.size(path);
                if (size < expectedLength) {
                    throw new IllegalArgumentException(
                            description + " file " + path + " is undersized: expected at least "
                                    + expectedLength + " bytes but found " + size);
                }
            } else {
                try (FileChannel channel = FileChannel.open(
                        path,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
                    channel.truncate(expectedLength);
                    channel.write(java.nio.ByteBuffer.wrap(new byte[] {0}), expectedLength - 1L);
                }
            }

            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0, expectedLength);
            }
        } catch (java.io.IOException ex) {
            throw new UncheckedIOException("Failed to map " + description + " file " + path, ex);
        }
    }
}
