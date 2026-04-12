package io.rueishi.marketdata.crypto.core.observability;

import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;

/**
 * Gateway facade for Agrona distinct error recording.
 *
 * <p>{@code GatewayErrorLog} is created by {@link ObservabilityRuntime} during
 * startup and wraps Agrona's {@link DistinctErrorLog} so parser, recovery, and
 * publisher hot paths can record repeated failures without knowing anything
 * about files, channels, or memory mapping. The underlying error log is
 * memory-mapped at the configured observability path and deduplicates repeated
 * exception observations.</p>
 *
 * <p>The facade owns the mapped error-log buffer for the lifetime of the
 * runtime. Callers typically receive this object from
 * {@link ObservabilityRuntime#errorLog()} and call {@link #record(Throwable)}
 * from failure paths that would otherwise risk allocation-heavy logging.</p>
 */
public final class GatewayErrorLog implements AutoCloseable {
    /** Default mapped error-log size until a later config card makes it tunable. */
    public static final int DEFAULT_ERROR_LOG_LENGTH = 1_048_576;

    private final Path path;
    private final MappedByteBuffer mappedBuffer;
    private final UnsafeBuffer errorBuffer;
    private final DistinctErrorLog errorLog;
    private boolean closed;

    /**
     * Maps the default gateway error-log size at the supplied path.
     *
     * @param path configured error-log file path
     * @return mapped gateway error-log facade
     * @throws NullPointerException if {@code path} is null
     * @throws IllegalArgumentException if {@code path} is blank
     * @throws UncheckedIOException if file creation, sizing, or mapping fails
     */
    public static GatewayErrorLog create(Path path) {
        return create(path, DEFAULT_ERROR_LOG_LENGTH, SystemEpochClock.INSTANCE);
    }

    /**
     * Maps an error log at the supplied path using an explicit clock.
     *
     * <p>The clock overload keeps tests deterministic while production startup
     * uses the system epoch clock. Existing files must be at least the requested
     * length so startup does not silently accept a truncated error-log mapping.</p>
     *
     * @param path configured error-log file path
     * @param length mapped error-log size in bytes
     * @param epochClock clock used by Agrona for first/last observation timestamps
     * @return mapped gateway error-log facade
     * @throws NullPointerException if {@code path} or {@code epochClock} is null
     * @throws IllegalArgumentException if {@code path} is blank or {@code length} is not positive
     * @throws UncheckedIOException if file creation, sizing, or mapping fails
     */
    public static GatewayErrorLog create(Path path, int length, EpochClock epochClock) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(epochClock, "epochClock");
        if (path.toString().isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }

        MappedByteBuffer mappedBuffer = mapExistingOrNew(path, length);
        return new GatewayErrorLog(path, mappedBuffer, epochClock);
    }

    private GatewayErrorLog(Path path, MappedByteBuffer mappedBuffer, EpochClock epochClock) {
        this.path = path;
        this.mappedBuffer = mappedBuffer;
        this.errorBuffer = new UnsafeBuffer(mappedBuffer);
        this.errorLog = new DistinctErrorLog(errorBuffer, epochClock);
    }

    /**
     * Records one failure observation in the distinct error log.
     *
     * <p>Agrona returns {@code false} when the mapped log has no room for the
     * encoded exception. The facade deliberately returns that signal to callers
     * instead of throwing on a full log so hot-path failure handling can remain
     * explicit and cheap.</p>
     *
     * @param error throwable to record
     * @return {@code true} if the observation was recorded; {@code false} if the log had no room
     * @throws NullPointerException if {@code error} is null
     * @throws IllegalStateException if the log has been closed
     */
    public boolean record(Throwable error) {
        Objects.requireNonNull(error, "error");
        ensureOpen();
        return errorLog.record(error);
    }

    /**
     * Returns the mapped error-log file path.
     *
     * @return configured error-log path
     */
    public Path path() {
        return path;
    }

    /**
     * Returns the Agrona buffer backing the distinct error log.
     *
     * <p>This accessor exists for verification and later diagnostics code. Hot
     * paths should use {@link #record(Throwable)} instead of writing into the
     * buffer directly.</p>
     *
     * @return mapped Agrona error buffer
     * @throws IllegalStateException if the log has been closed
     */
    public UnsafeBuffer buffer() {
        ensureOpen();
        return errorBuffer;
    }

    /**
     * Forces dirty error-log pages to disk and unmaps the underlying buffer.
     *
     * <p>The method is idempotent and is called by {@link ObservabilityRuntime}
     * during normal shutdown or failed bootstrap cleanup.</p>
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        mappedBuffer.force();
        IoUtil.unmap(mappedBuffer);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("gateway error log is closed");
        }
    }

    private static MappedByteBuffer mapExistingOrNew(Path path, int expectedLength) {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            if (Files.exists(path)) {
                long size = Files.size(path);
                if (size < expectedLength) {
                    throw new IllegalArgumentException(
                            "error log file " + path + " is undersized: expected at least "
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
            throw new UncheckedIOException("Failed to map error log file " + path, ex);
        }
    }
}
