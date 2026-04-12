package io.rueishi.marketdata.crypto.core.observability;

import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import java.nio.file.Path;
import java.util.Objects;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Closeable owner for the gateway's shared observability resources.
 *
 * <p>{@code ObservabilityRuntime} is startup wiring created by bootstrap after
 * config validation and before connector creation. It owns the memory-mapped
 * counter storage, the shared {@link CountersManager}/{@link CountersReader},
 * the {@link GatewayCounters} facade passed into connector contexts, and the
 * {@link GatewayErrorLog} used for hot-path distinct error recording.</p>
 *
 * <p>The runtime separates ownership from usage: connectors increment counters
 * through {@code GatewayCounters} and later components can read all counters
 * generically through {@link #countersReader()}, while this object closes the
 * mapped files in the correct order during normal shutdown or failed bootstrap
 * cleanup.</p>
 */
public final class ObservabilityRuntime implements AutoCloseable {
    private final MappedCountersFactory countersFactory;
    private final GatewayCounters counters;
    private final GatewayErrorLog errorLog;
    private boolean closed;

    /**
     * Creates all shared observability resources from validated startup config.
     *
     * @param config observability configuration with counter and error-log paths
     * @param instanceId gateway instance label for counter allocation
     * @param environment environment label for counter allocation
     * @param venue venue label for counter allocation
     * @return initialized observability runtime ready for connector creation
     * @throws NullPointerException if any argument is null
     * @throws IllegalArgumentException if required labels or paths are invalid
     * @throws java.io.UncheckedIOException if mapped-file creation fails
     */
    public static ObservabilityRuntime create(
            ObservabilityConfig config,
            String instanceId,
            String environment,
            String venue) {
        Objects.requireNonNull(config, "config");
        MappedCountersFactory countersFactory = null;
        GatewayErrorLog errorLog = null;
        try {
            countersFactory = MappedCountersFactory.create(Path.of(config.countersSharedMemoryPath));
            errorLog = GatewayErrorLog.create(Path.of(config.errorLogPath));
            GatewayCounters counters = new GatewayCounters(
                    countersFactory.countersManager(),
                    instanceId,
                    environment,
                    venue);
            return new ObservabilityRuntime(countersFactory, counters, errorLog);
        } catch (RuntimeException ex) {
            if (errorLog != null) {
                errorLog.close();
            }
            if (countersFactory != null) {
                countersFactory.close();
            }
            throw ex;
        }
    }

    private ObservabilityRuntime(
            MappedCountersFactory countersFactory,
            GatewayCounters counters,
            GatewayErrorLog errorLog) {
        this.countersFactory = Objects.requireNonNull(countersFactory, "countersFactory");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.errorLog = Objects.requireNonNull(errorLog, "errorLog");
    }

    /**
     * Returns the gateway counter facade passed to connector contexts.
     *
     * @return shared gateway counters
     * @throws IllegalStateException if the runtime has already been closed
     */
    public GatewayCounters counters() {
        ensureOpen();
        return counters;
    }

    /**
     * Returns the shared counter manager used for new counter allocation.
     *
     * @return mapped Agrona counter manager
     * @throws IllegalStateException if the runtime has already been closed
     */
    public CountersManager countersManager() {
        ensureOpen();
        return countersFactory.countersManager();
    }

    /**
     * Returns the generic counter reader over all shared counter allocations.
     *
     * <p>Metrics export uses this reader so newly allocated gateway or
     * instrument counters become visible without endpoint-specific
     * registration.</p>
     *
     * @return mapped Agrona counter reader
     * @throws IllegalStateException if the runtime has already been closed
     */
    public CountersReader countersReader() {
        ensureOpen();
        return countersFactory.countersReader();
    }

    /**
     * Returns the distinct error-log facade for repeated hot-path failures.
     *
     * @return mapped gateway error log
     * @throws IllegalStateException if the runtime has already been closed
     */
    public GatewayErrorLog errorLog() {
        ensureOpen();
        return errorLog;
    }

    /**
     * Closes counters, the mapped error log, and the mapped counter files.
     *
     * <p>The method is idempotent. Counter handles are released before mapped
     * buffers are unmapped so no live {@code AtomicCounter} references point at
     * unmapped storage during shutdown.</p>
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        counters.close();
        errorLog.close();
        countersFactory.close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("observability runtime is closed");
        }
    }
}
