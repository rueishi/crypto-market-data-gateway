package io.rueishi.marketdata.crypto.core.bootstrap;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.MetricsEndpoint;
import io.rueishi.marketdata.crypto.core.observability.ObservabilityRuntime;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestRouter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Closeable owner for a fully initialized gateway runtime.
 *
 * <p>{@code GatewayRuntime} is created by {@link GatewayBootstrap} after
 * configuration validation, observability mapping, connector construction, and
 * metrics endpoint startup. It owns the connector list, shared publisher,
 * metrics endpoint, connector-local event-loop groups, optional CPU affinity
 * locks, and the shared {@link ObservabilityRuntime}. Downstream control planes
 * route recovery through {@link #requestRecovery(RecoveryRequest)} while the
 * runtime is active.</p>
 *
 * <p>Shutdown is idempotent and deadline-aware. It stops accepting new recovery
 * routing, stops metrics scraping, asks every connector to unsubscribe/reset/
 * close, releases affinity locks, awaits event-loop shutdown only until the
 * configured deadline expires, and then closes observability resources so mapped
 * files are flushed and readable by external diagnostics.</p>
 */
public class GatewayRuntime implements AutoCloseable {
    private final GatewayConfig config;
    private final ObservabilityRuntime observabilityRuntime;
    private final MetricsEndpoint metricsEndpoint;
    private final RecoveryRequestRouter recoveryRequestRouter;
    private final Publisher publisher;
    private final List<Connector> connectors;
    private final List<EventLoopGroup> eventLoopGroups;
    private final List<AutoCloseable> affinityLocks;
    private boolean shuttingDown;
    private boolean closed;
    private boolean shutdownDeadlineExceeded;
    private Thread shutdownHook;

    GatewayRuntime(
            GatewayConfig config,
            ObservabilityRuntime observabilityRuntime,
            MetricsEndpoint metricsEndpoint,
            Publisher publisher,
            List<Connector> connectors,
            List<EventLoopGroup> eventLoopGroups,
            List<AutoCloseable> affinityLocks) {
        this.config = Objects.requireNonNull(config, "config");
        this.observabilityRuntime = Objects.requireNonNull(observabilityRuntime, "observabilityRuntime");
        this.metricsEndpoint = Objects.requireNonNull(metricsEndpoint, "metricsEndpoint");
        this.recoveryRequestRouter = new RecoveryRequestRouter(Objects.requireNonNull(connectors, "connectors"));
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.connectors = List.copyOf(connectors);
        this.eventLoopGroups = List.copyOf(eventLoopGroups);
        this.affinityLocks = List.copyOf(affinityLocks);
    }

    /**
     * Returns the validated startup configuration used to create this runtime.
     *
     * @return validated gateway configuration
     */
    public GatewayConfig config() {
        return config;
    }

    /**
     * Returns the shared gateway counters facade.
     *
     * @return counters retained for connector use and shutdown
     */
    public GatewayCounters counters() {
        return observabilityRuntime.counters();
    }

    /**
     * Returns the shared observability runtime retained by bootstrap.
     *
     * @return observability runtime owning mapped counters and error log
     */
    public ObservabilityRuntime observabilityRuntime() {
        return observabilityRuntime;
    }

    /**
     * Returns the metrics endpoint started during bootstrap.
     *
     * @return non-hot-path Prometheus metrics endpoint
     */
    public MetricsEndpoint metricsEndpoint() {
        return metricsEndpoint;
    }

    /**
     * Routes a downstream recovery request to the owning connector unless shutdown is draining.
     *
     * <p>Once graceful shutdown starts, new recovery work is rejected so no
     * reconnect or resubscribe can race with connector shutdown. Requests for
     * another venue or an unknown instrument are also ignored and reported as
     * {@code false}.</p>
     *
     * @param request downstream recovery request metadata
     * @return {@code true} if a connector route was found and accepted, otherwise {@code false}
     * @throws NullPointerException if {@code request} is null
     */
    public synchronized boolean requestRecovery(RecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        if (shuttingDown || closed) {
            return false;
        }
        return recoveryRequestRouter.route(request);
    }

    /**
     * Returns the shared publisher instance selected by bootstrap.
     *
     * @return publisher injected into every connector context
     */
    public Publisher publisher() {
        return publisher;
    }

    /**
     * Returns initialized connectors in instrument configuration order.
     *
     * @return immutable connector list
     */
    public List<Connector> connectors() {
        return connectors;
    }

    /**
     * Returns connector-local event loop groups owned by this runtime.
     *
     * @return immutable event loop group list
     */
    public List<EventLoopGroup> eventLoopGroups() {
        return eventLoopGroups;
    }

    /**
     * Returns whether the event-loop shutdown deadline was exceeded.
     *
     * @return true when at least one event-loop group failed to terminate before the configured deadline elapsed
     */
    public boolean shutdownDeadlineExceeded() {
        return shutdownDeadlineExceeded;
    }

    /**
     * Installs a JVM shutdown hook that delegates SIGTERM-style termination to graceful shutdown.
     *
     * <p>The repository currently has no production {@code main} entry point,
     * so bootstrap does not register hooks implicitly during tests. A future
     * executable startup path can call this method after initialization; the JVM
     * will then invoke {@link #shutdownGracefully()} during normal shutdown-hook
     * processing for SIGTERM or process exit. The method is idempotent and
     * returns the installed hook thread for test or diagnostic visibility.</p>
     *
     * @return installed shutdown hook thread
     * @throws IllegalStateException if the JVM is already shutting down and rejects hook registration
     */
    public synchronized Thread installShutdownHook() {
        if (shutdownHook == null) {
            shutdownHook = new Thread(this::shutdownGracefully, "crypto-market-data-gateway-shutdown");
            java.lang.Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
        return shutdownHook;
    }

    /**
     * Performs deadline-aware graceful shutdown.
     *
     * <p>The method is idempotent. Connector shutdown exceptions and affinity
     * release failures are recorded in the mapped error log when possible, then
     * shutdown continues for the remaining resources. Event-loop groups are
     * asked to stop with the remaining deadline; if the deadline expires, the
     * runtime stops waiting and records the breach.</p>
     */
    public synchronized void shutdownGracefully() {
        if (closed) {
            return;
        }
        shuttingDown = true;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.transport.shutdownDeadlineMs);

        metricsEndpoint.close();
        for (Connector connector : connectors) {
            try {
                connector.shutdown();
            } catch (RuntimeException ex) {
                recordShutdownError(ex);
            }
        }
        releaseAffinityLocks();
        awaitEventLoopShutdown(deadlineNanos);
        closePublisher();
        closed = true;
        observabilityRuntime.close();
    }

    /**
     * Closes this runtime through the same graceful shutdown path.
     */
    @Override
    public void close() {
        shutdownGracefully();
    }

    private void releaseAffinityLocks() {
        for (AutoCloseable lock : affinityLocks) {
            try {
                lock.close();
            } catch (Exception ex) {
                recordShutdownError(ex);
            }
        }
    }

    private void awaitEventLoopShutdown(long deadlineNanos) {
        for (EventLoopGroup eventLoopGroup : eventLoopGroups) {
            long remainingMillis = remainingMillis(deadlineNanos);
            if (remainingMillis <= 0) {
                markDeadlineExceeded();
                return;
            }
            Future<?> shutdown = eventLoopGroup.shutdownGracefully(0L, remainingMillis, TimeUnit.MILLISECONDS);
            if (!shutdown.awaitUninterruptibly(remainingMillis, TimeUnit.MILLISECONDS)) {
                markDeadlineExceeded();
                return;
            }
        }
    }

    private void closePublisher() {
        if (publisher instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ex) {
                recordShutdownError(ex);
            }
        }
    }

    private long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return 0L;
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    private void markDeadlineExceeded() {
        shutdownDeadlineExceeded = true;
        recordShutdownError(new IllegalStateException("Gateway shutdown deadline exceeded"));
    }

    private void recordShutdownError(Throwable error) {
        try {
            observabilityRuntime.errorLog().record(error);
        } catch (RuntimeException ignored) {
            // Shutdown must continue even if observability is already full or closing.
        }
    }
}
