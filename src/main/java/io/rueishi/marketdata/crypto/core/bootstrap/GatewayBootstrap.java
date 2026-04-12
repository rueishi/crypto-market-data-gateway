package io.rueishi.marketdata.crypto.core.bootstrap;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.ConfigLoader;
import io.rueishi.marketdata.crypto.core.config.ConfigValidator;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherType;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import io.rueishi.marketdata.crypto.core.connector.DefaultConnectorContext;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.MetricsEndpoint;
import io.rueishi.marketdata.crypto.core.observability.ObservabilityRuntime;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.publisher.LoggingPublisher;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.transport.CpuAffinitySupport;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;

/**
 * Startup orchestrator for validated configuration and connector initialization.
 *
 * <p>{@code GatewayBootstrap} ties together the core components produced by
 * earlier task cards: {@link ConfigLoader} and {@link ConfigValidator} provide
 * the validated {@link GatewayConfig}, {@link VenueRegistry} resolves the
 * venue-specific {@link ConnectorFactory} through ServiceLoader, and the
 * bootstrap creates the shared {@link ObservabilityRuntime}, a publisher,
 * connector-local event loop groups, one {@link ConnectorContext} per
 * instrument, and the non-hot-path {@link MetricsEndpoint}. It then creates
 * and initializes one connector for every configured instrument before
 * starting metrics scraping.</p>
 *
 * <p>The bootstrap stops after initialization and resource retention in the
 * current executable path; it does not call {@link Connector#connect()}. It now
 * allocates socket-capable connector event loop groups so later orchestration
 * can start the returned connectors without rebuilding their runtime context.</p>
 */
public final class GatewayBootstrap {
    private final VenueRegistry venueRegistry;

    /**
     * Creates a bootstrap that discovers connector factories from ServiceLoader.
     */
    public GatewayBootstrap() {
        this(new VenueRegistry());
    }

    /**
     * Creates a bootstrap with an explicit registry.
     *
     * <p>Tests use this constructor to inject a registry with controlled
     * factories. Production startup uses the no-argument constructor so factory
     * discovery remains ServiceLoader-based.</p>
     *
     * @param venueRegistry registry used to resolve connector factories
     * @throws NullPointerException if {@code venueRegistry} is null
     */
    public GatewayBootstrap(VenueRegistry venueRegistry) {
        this.venueRegistry = Objects.requireNonNull(venueRegistry, "venueRegistry");
    }

    /**
     * Loads configuration from a path and initializes the gateway runtime.
     *
     * <p>This is the filesystem-oriented entry point for application startup.
     * Configuration loading performs validation before this method allocates
     * connector resources or resolves venue factories.</p>
     *
     * @param configPath path to gateway YAML
     * @return initialized runtime handle
     * @throws IOException if the config file cannot be read
     * @throws IllegalArgumentException if configuration is invalid or no connector factory is registered
     */
    public Runtime initialize(Path configPath) throws IOException {
        return initialize(ConfigLoader.load(configPath));
    }

    /**
     * Initializes the gateway runtime from an already bound configuration object.
     *
     * <p>The method validates the supplied object first, then creates shared
     * resources, resolves the connector factory for the configured venue, and
     * constructs one connector per instrument. If connector creation or
     * initialization fails, any resources allocated so far are closed before the
     * exception is rethrown.</p>
     *
     * @param config bound gateway configuration
     * @return initialized runtime handle containing connectors and owned resources
     * @throws NullPointerException if {@code config} is null
     * @throws IllegalArgumentException if validation, publisher selection, or registry resolution fails
     */
    public Runtime initialize(GatewayConfig config) {
        GatewayConfig validatedConfig = ConfigValidator.validate(Objects.requireNonNull(config, "config"));
        ObservabilityRuntime observabilityRuntime = null;
        MetricsEndpoint metricsEndpoint = null;
        List<EventLoopGroup> eventLoopGroups = new ArrayList<>();
        List<AutoCloseable> affinityLocks = new ArrayList<>();
        try {
            Publisher publisher = resolvePublisher(validatedConfig);
            observabilityRuntime = ObservabilityRuntime.create(
                    validatedConfig.observability,
                    validatedConfig.instanceId,
                    validatedConfig.environment,
                    validatedConfig.venue.name());
            ConnectorFactory factory = venueRegistry.forVenue(validatedConfig.venue);
            List<Connector> connectors = initializeConnectors(
                    validatedConfig,
                    factory,
                    observabilityRuntime.counters(),
                    publisher,
                    eventLoopGroups,
                    affinityLocks);
            metricsEndpoint = new MetricsEndpoint(
                    observabilityRuntime.countersReader(),
                    validatedConfig.observability.metricsHttpPort,
                    observabilityRuntime.errorLog());
            metricsEndpoint.start();
            return new Runtime(
                    validatedConfig,
                    observabilityRuntime,
                    metricsEndpoint,
                    publisher,
                    List.copyOf(connectors),
                    List.copyOf(eventLoopGroups),
                    List.copyOf(affinityLocks));
        } catch (RuntimeException ex) {
            if (metricsEndpoint != null) {
                metricsEndpoint.close();
            }
            closeAffinityLocks(affinityLocks);
            closeEventLoopGroups(eventLoopGroups);
            if (observabilityRuntime != null) {
                observabilityRuntime.close();
            }
            throw ex;
        }
    }

    /**
     * Resolves the configured publisher implementation for the current bootstrap runtime.
     *
     * <p>The bootstrap runtime supports the in-memory capture publisher and
     * the Log4j 2 async logging publisher. Other enum values remain accepted by
     * configuration validation for later publisher cards, but fail explicitly
     * here instead of pretending a placeholder publisher can carry messages.</p>
     *
     * @param config validated gateway configuration
     * @return shared publisher instance for all connectors
     * @throws IllegalArgumentException if the publisher type is not usable in bootstrap
     */
    private Publisher resolvePublisher(GatewayConfig config) {
        PublisherType publisherType = ConfigValidator.validatePublisher(config.publisher);
        if (publisherType == PublisherType.IN_MEMORY) {
            return new InMemoryPublisher();
        }
        if (publisherType == PublisherType.LOGGING) {
            return new LoggingPublisher(config.publisher.logging);
        }
        throw new IllegalArgumentException(
                "Publisher type " + publisherType + " is not available in bootstrap; use IN_MEMORY or LOGGING");
    }

    /**
     * Creates and initializes one connector for each configured instrument.
     *
     * @param config validated gateway configuration
     * @param factory resolved connector factory
     * @param counters shared gateway counters
     * @param publisher shared publisher
     * @param eventLoopGroups mutable list that receives every owned event loop group
     * @return initialized connector list in instrument configuration order
     */
    private List<Connector> initializeConnectors(
            GatewayConfig config,
            ConnectorFactory factory,
            GatewayCounters counters,
            Publisher publisher,
            List<EventLoopGroup> eventLoopGroups,
            List<AutoCloseable> affinityLocks) {
        List<Connector> connectors = new ArrayList<>(config.instruments.size());
        EpochClock epochClock = SystemEpochClock.INSTANCE;
        for (InstrumentConfig instrument : config.instruments) {
            EventLoopGroup eventLoopGroup = newConnectorEventLoopGroup(config.transport);
            eventLoopGroups.add(eventLoopGroup);
            affinityLocks.add(CpuAffinitySupport.acquire(
                    eventLoopGroup,
                    config.transport.cpuAffinity,
                    config.transport.busyWaitEnabled,
                    connectors.size()));
            org.agrona.concurrent.CachedNanoClock nanoClock = new org.agrona.concurrent.CachedNanoClock();
            ConnectorContext context = new DefaultConnectorContext(
                    counters,
                    publisher,
                    nanoClock,
                    epochClock,
                    config.transport,
                    eventLoopGroup,
                    config.venueConfig);
            Connector connector = Objects.requireNonNull(
                    factory.create(instrument, config),
                    "factory.create");
            connector.init(context);
            connectors.add(connector);
        }
        return connectors;
    }

    /**
     * Creates the connector-local Netty event loop group selected by the transport policy.
     *
     * <p>{@link io.rueishi.marketdata.crypto.core.config.IoTransport#AUTO} prefers
     * {@code io_uring} when the incubator native library is on the classpath and the
     * kernel supports it, then falls back to {@code epoll} on Linux, then to portable
     * NIO on non-Linux environments (developer machines, CI runners).
     * {@link io.rueishi.marketdata.crypto.core.config.IoTransport#EPOLL} forces epoll
     * and fails startup if unavailable.
     * {@link io.rueishi.marketdata.crypto.core.config.IoTransport#IO_URING} requires
     * io_uring and fails startup if unavailable.</p>
     *
     * @param transport transport configuration supplying the selection policy
     * @return connector-owned event loop group with one thread
     * @throws IllegalStateException if the explicitly requested transport is unavailable
     */
    private EventLoopGroup newConnectorEventLoopGroup(
            io.rueishi.marketdata.crypto.core.config.TransportConfig transport) {
        io.rueishi.marketdata.crypto.core.config.IoTransport policy =
                transport.ioTransport != null
                        ? transport.ioTransport
                        : io.rueishi.marketdata.crypto.core.config.IoTransport.AUTO;
        return switch (policy) {
            case IO_URING -> {
                if (!isIoUringAvailable()) {
                    throw new IllegalStateException(
                            "transport.ioTransport=IO_URING but io_uring is not available. "
                            + "Upgrade to kernel 5.9+, add netty-incubator-transport-native-iouring "
                            + "to the classpath, or set transport.ioTransport=AUTO to allow fallback.");
                }
                yield newIoUringEventLoopGroup();
            }
            case EPOLL -> {
                if (!io.netty.channel.epoll.Epoll.isAvailable()) {
                    throw new IllegalStateException(
                            "transport.ioTransport=EPOLL but epoll is not available (not Linux?). "
                            + "Use transport.ioTransport=AUTO to fall back to NIO on non-Linux systems.");
                }
                yield new io.netty.channel.epoll.EpollEventLoopGroup(1);
            }
            case AUTO -> {
                if (isIoUringAvailable()) {
                    yield newIoUringEventLoopGroup();
                } else if (io.netty.channel.epoll.Epoll.isAvailable()) {
                    yield new io.netty.channel.epoll.EpollEventLoopGroup(1);
                } else {
                    yield new NioEventLoopGroup(1);
                }
            }
        };
    }

    /**
     * Returns true when the io_uring incubator native library is on the classpath
     * and the kernel supports io_uring. Uses reflection to avoid a hard compile-time
     * dependency on the optional incubator artifact.
     */
    private static boolean isIoUringAvailable() {
        try {
            Class<?> ioUring = Class.forName("io.netty.incubator.channel.uring.IOUring");
            return (boolean) ioUring.getMethod("isAvailable").invoke(null);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Creates an IOUringEventLoopGroup via reflection to avoid a hard compile-time
     * dependency on the optional incubator artifact.
     */
    private static EventLoopGroup newIoUringEventLoopGroup() {
        try {
            Class<?> groupClass = Class.forName(
                    "io.netty.incubator.channel.uring.IOUringEventLoopGroup");
            return (EventLoopGroup) groupClass.getConstructor(int.class).newInstance(1);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to instantiate IOUringEventLoopGroup", e);
        }
    }

    /**
     * Requests graceful shutdown for every connector-local event loop group.
     *
     * <p>Bootstrap uses this both on failed initialization and normal runtime
     * close. The shutdown waits for each event loop to terminate so scheduled
     * recovery tasks cannot continue to mutate counters after observability
     * storage has been unmapped.</p>
     *
     * @param eventLoopGroups event loop groups allocated so far
     */
    private static void closeEventLoopGroups(List<EventLoopGroup> eventLoopGroups) {
        for (EventLoopGroup eventLoopGroup : eventLoopGroups) {
            eventLoopGroup.shutdownGracefully(0L, 1_000L, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .syncUninterruptibly();
        }
    }

    private static void closeAffinityLocks(List<AutoCloseable> affinityLocks) {
        for (AutoCloseable affinityLock : affinityLocks) {
            try {
                affinityLock.close();
            } catch (Exception ignored) {
                // Failed startup cleanup must continue to release event loops and observability.
            }
        }
    }

    /**
     * Process entry point. Declared as {@code Main-Class} in the JAR manifest.
     *
     * <p>Configuration is supplied entirely via the JVM system property
     * {@code -Dgateway.config=<path>}. Command-line args are intentionally
     * ignored — all configuration lives in the YAML file.</p>
     *
     * <p>Exits with status 1 on any startup failure so the process supervisor
     * (systemd, Kubernetes, etc.) can detect and restart the process.</p>
     *
     * @param args ignored — use {@code -Dgateway.config=<path>} instead
     */
    public static void main(String[] args) {
        String configPath = System.getProperty("gateway.config");
        if (configPath == null || configPath.isBlank()) {
            System.err.println("[FATAL] -Dgateway.config=<path> is required");
            System.exit(1);
        }
        Runtime runtime = null;
        try {
            runtime = new GatewayBootstrap().initialize(java.nio.file.Path.of(configPath));
            // Register SIGTERM shutdown hook before connecting so partial-connect state is cleaned up
            runtime.installShutdownHook();
            // Connect every connector: dial → TLS → WS upgrade → subscribe
            for (io.rueishi.marketdata.crypto.core.connector.Connector connector : runtime.connectors()) {
                connector.connect();
            }
            // Keep the process alive. SIGTERM triggers the installed shutdown hook.
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (runtime != null) {
                runtime.shutdownGracefully();
            }
        } catch (Exception e) {
            System.err.println("[FATAL] Startup failed: " + e.getMessage());
            e.printStackTrace(System.err);
            if (runtime != null) {
                runtime.shutdownGracefully();
            }
            System.exit(1);
        }
    }

    /**
     * Backward-compatible runtime type returned by existing bootstrap entry points.
     *
     * <p>The concrete lifecycle implementation now lives in {@link GatewayRuntime}
     * so production and tests can refer to it directly. This nested type remains
     * as the return type for existing callers that already use
     * {@code GatewayBootstrap.Runtime}.</p>
     */
    public static final class Runtime extends GatewayRuntime {
        private Runtime(
                GatewayConfig config,
                ObservabilityRuntime observabilityRuntime,
                MetricsEndpoint metricsEndpoint,
                Publisher publisher,
                List<Connector> connectors,
                List<EventLoopGroup> eventLoopGroups,
                List<AutoCloseable> affinityLocks) {
            super(config, observabilityRuntime, metricsEndpoint, publisher, connectors, eventLoopGroups, affinityLocks);
        }
    }
}
