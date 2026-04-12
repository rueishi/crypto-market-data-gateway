package io.rueishi.marketdata.crypto.core.config;

/**
 * Observability configuration loaded during gateway startup.
 *
 * <p>Bootstrap uses this object to locate counter and error-log storage and to
 * reserve the metrics HTTP port for later endpoint work. These values are
 * startup/runtime wiring concerns and are not read by parser hot-path code.</p>
 */
public final class ObservabilityConfig {
    /** Path to shared-memory counter storage. */
    public String countersSharedMemoryPath;
    /** Path to the gateway error log. */
    public String errorLogPath;
    /** HTTP port reserved for metrics scraping. */
    public int metricsHttpPort;
}
