package io.rueishi.marketdata.crypto.core.observability;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.status.CountersReader;

/**
 * Non-hot-path HTTP endpoint for Prometheus counter scraping.
 *
 * <p>{@code MetricsEndpoint} is created by {@link io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap}
 * after {@link ObservabilityRuntime} has initialized shared mapped counters.
 * It owns a small JDK HTTP server bound to the configured metrics port and
 * renders `/metrics` by iterating the shared {@link CountersReader} at scrape
 * time through {@link PrometheusCounterFormatter}. Because it reads the
 * generic counter store on every request, counters allocated later through the
 * shared manager appear automatically without endpoint-specific registration.</p>
 *
 * <p>The endpoint runs on a dedicated daemon thread outside parser, publisher,
 * connector, and recovery hot paths. Bootstrap starts it after connector
 * initialization and stops it from the runtime close path; tests may also start
 * it directly on port {@code 0} to request an OS-assigned free port.</p>
 */
public final class MetricsEndpoint implements AutoCloseable {
    /** Prometheus 0.0.4 content type required by the scrape contract. */
    public static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private static final String METRICS_PATH = "/metrics";

    private final CountersReader countersReader;
    private final int httpPort;
    private final GatewayErrorLog errorLog;
    private final PrometheusCounterFormatter formatter;

    private HttpServer server;
    private ExecutorService executor;

    /**
     * Creates an endpoint that scrapes the supplied generic counter reader.
     *
     * @param countersReader shared Agrona counter reader supplied by the observability runtime
     * @param httpPort HTTP port to bind, or {@code 0} for an OS-assigned test port
     * @throws NullPointerException if {@code countersReader} is null
     * @throws IllegalArgumentException if {@code httpPort} is outside the TCP port range
     */
    public MetricsEndpoint(CountersReader countersReader, int httpPort) {
        this(countersReader, httpPort, null, new PrometheusCounterFormatter());
    }

    /**
     * Creates an endpoint with an optional error log for scrape failures.
     *
     * <p>{@code GatewayBootstrap} uses this constructor so formatter or I/O
     * failures can be recorded through the Phase 3 distinct error log without
     * coupling the formatter to mapped-file details.</p>
     *
     * @param countersReader shared Agrona counter reader supplied by the observability runtime
     * @param httpPort HTTP port to bind, or {@code 0} for an OS-assigned test port
     * @param errorLog optional gateway error log used to record unexpected scrape failures
     * @throws NullPointerException if {@code countersReader} is null
     * @throws IllegalArgumentException if {@code httpPort} is outside the TCP port range
     */
    public MetricsEndpoint(CountersReader countersReader, int httpPort, GatewayErrorLog errorLog) {
        this(countersReader, httpPort, errorLog, new PrometheusCounterFormatter());
    }

    MetricsEndpoint(
            CountersReader countersReader,
            int httpPort,
            GatewayErrorLog errorLog,
            PrometheusCounterFormatter formatter) {
        this.countersReader = Objects.requireNonNull(countersReader, "countersReader");
        if (httpPort < 0 || httpPort > 65_535) {
            throw new IllegalArgumentException("httpPort must be between 0 and 65535");
        }
        this.httpPort = httpPort;
        this.errorLog = errorLog;
        this.formatter = Objects.requireNonNull(formatter, "formatter");
    }

    /**
     * Starts the dedicated HTTP server if it is not already running.
     *
     * <p>The method is idempotent. Binding failures are surfaced as
     * {@link UncheckedIOException} so bootstrap startup fails and its normal
     * cleanup path closes resources allocated before the endpoint.</p>
     *
     * @throws UncheckedIOException if the configured port cannot be bound
     */
    public synchronized void start() {
        if (server != null) {
            return;
        }

        try {
            HttpServer createdServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
            ExecutorService createdExecutor = Executors.newSingleThreadExecutor(new MetricsThreadFactory());
            createdServer.createContext(METRICS_PATH, this::handleMetrics);
            createdServer.setExecutor(createdExecutor);
            createdServer.start();
            server = createdServer;
            executor = createdExecutor;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to start metrics endpoint on port " + httpPort, ex);
        }
    }

    /**
     * Stops the HTTP server if it is running.
     *
     * <p>The method is idempotent and is called during runtime shutdown. It
     * stops the server immediately and shuts down the dedicated executor thread;
     * no parser or publisher hot-path resources are touched.</p>
     */
    public synchronized void stop() {
        if (server == null) {
            return;
        }

        server.stop(0);
        server = null;
        executor.shutdownNow();
        executor = null;
    }

    /**
     * Returns the configured HTTP port requested at construction time.
     *
     * @return configured metrics HTTP port
     */
    public int httpPort() {
        return httpPort;
    }

    /**
     * Returns the actual bound port.
     *
     * <p>This is usually the same as {@link #httpPort()}, but tests that bind
     * port {@code 0} use this value to discover the OS-assigned port after
     * {@link #start()}.</p>
     *
     * @return bound server port if running, otherwise the configured port
     */
    public synchronized int boundPort() {
        return server == null ? httpPort : server.getAddress().getPort();
    }

    /**
     * Returns the generic counter reader used for scrape-time discovery.
     *
     * @return shared Agrona counter reader
     */
    public CountersReader countersReader() {
        return countersReader;
    }

    /**
     * Stops the endpoint as part of {@link AutoCloseable} runtime shutdown.
     */
    @Override
    public void close() {
        stop();
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        try {
            if (!METRICS_PATH.equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, "not found\n", "text/plain; charset=utf-8");
                return;
            }

            String body = formatter.format(countersReader);
            send(exchange, 200, body, CONTENT_TYPE);
        } catch (RuntimeException ex) {
            if (errorLog != null) {
                errorLog.record(ex);
            }
            send(exchange, 500, "metrics scrape failed\n", "text/plain; charset=utf-8");
        } finally {
            exchange.close();
        }
    }

    private static void send(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static final class MetricsThreadFactory implements ThreadFactory {
        private final AtomicInteger nextId = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "gateway-metrics-http-" + nextId.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
