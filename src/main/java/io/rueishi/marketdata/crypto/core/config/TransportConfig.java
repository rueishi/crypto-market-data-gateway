package io.rueishi.marketdata.crypto.core.config;

/**
 * Transport configuration loaded once during gateway startup.
 *
 * <p>Later bootstrap and connector code use this object to configure connection
 * timeouts, reconnect backoff, heartbeat/liveness limits, frame size limits,
 * optional CPU affinity, and graceful shutdown behavior. It is not used on the
 * message parser hot path.</p>
 */
public final class TransportConfig {
    /** Connection attempt timeout in milliseconds. */
    public int connectTimeoutMs;
    /** Base reconnect backoff in milliseconds. */
    public long reconnectBackoffBaseMs;
    /** Maximum reconnect backoff in milliseconds. */
    public long reconnectBackoffMaxMs;
    /** Reconnect jitter bound in milliseconds. */
    public long reconnectBackoffJitterMs;
    /** Maximum heartbeat silence before liveness failure in milliseconds. */
    public long heartbeatTimeoutMs;
    /** Maximum accepted inbound frame size in bytes. */
    public int frameSizeLimitBytes;
    /** Whether the connector may use busy-wait behavior with suitable CPU pinning. */
    public boolean busyWaitEnabled;
    /** Optional CPU pinning configuration for connector event-loop threads. */
    public CpuAffinityConfig cpuAffinity = new CpuAffinityConfig();
    /** Maximum graceful shutdown wait in milliseconds. */
    public long shutdownDeadlineMs;
    /**
     * I/O transport selection policy for connector event-loop groups.
     * Defaults to {@link IoTransport#AUTO}: prefer {@code io_uring}, fall back
     * to {@code epoll}, then NIO on non-Linux environments.
     */
    public IoTransport ioTransport = IoTransport.AUTO;
}
