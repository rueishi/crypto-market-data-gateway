package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only mock Coinbase L3 REST API used by the Phase 4 end-to-end suite.
 *
 * <p>The mock wraps a JDK {@link HttpServer} bound to an ephemeral localhost port and serves a
 * configurable JSON body on every {@code GET /products/{product_id}/book?level=3} request. It
 * is constructed once per test scenario and its {@link #baseUrl()} is injected into the raw
 * Coinbase venue config map through the {@code restApiBase} key. The production
 * {@link CoinbaseL3SnapshotStrategy} then directs its REST snapshot fetches at this local
 * server instead of the real Coinbase Exchange REST endpoint.</p>
 *
 * <p>Tests may call {@link #setResponseBody(String)} between scenarios to vary the snapshot
 * sequence number for alignment and recovery testing without starting a new server. The
 * {@link #requestCount()} accessor lets tests verify how many REST fetches the snapshot
 * strategy issued.</p>
 *
 * <p>This helper is package-private and intentionally lives in test sources so no
 * venue-agnostic core code ever depends on the REST mock.</p>
 */
final class MockL3RestServer implements AutoCloseable {

    private final HttpServer server;
    private final AtomicReference<String> responseBody;
    private final AtomicInteger requestCount = new AtomicInteger();
    private final String baseUrl;

    /**
     * Starts an in-process mock REST server bound to an ephemeral localhost port.
     *
     * <p>The server matches any {@code /products/...} path so the snapshot strategy's full
     * {@code /products/BTC-USD/book?level=3} request routes to the same handler. Other paths
     * return {@code 404}. The initial response body is taken from the supplied parameter; tests
     * can override it mid-scenario via {@link #setResponseBody(String)}.</p>
     *
     * @param initialResponseBody initial REST snapshot JSON body to return
     * @throws IOException if the underlying {@link HttpServer} cannot bind to localhost
     * @throws NullPointerException if {@code initialResponseBody} is null
     */
    MockL3RestServer(String initialResponseBody) throws IOException {
        if (initialResponseBody == null) {
            throw new NullPointerException("initialResponseBody");
        }
        this.responseBody = new AtomicReference<>(initialResponseBody);
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/products", exchange -> {
            requestCount.incrementAndGet();
            byte[] bytes = responseBody.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        this.server.start();
        int boundPort = server.getAddress().getPort();
        this.baseUrl = "http://127.0.0.1:" + boundPort;
    }

    /**
     * Loads a classpath fixture as a UTF-8 string and wraps it in a new mock REST server.
     *
     * @param fixturePath path to a JSON fixture under the test resources tree
     * @return started mock REST server serving the fixture body
     * @throws IOException if the fixture cannot be read or the server cannot bind
     */
    static MockL3RestServer withFixture(Path fixturePath) throws IOException {
        return new MockL3RestServer(Files.readString(fixturePath));
    }

    /**
     * Returns the base URL for the mock REST server.
     *
     * <p>The snapshot strategy concatenates this with the path
     * {@code /products/{product_id}/book?level=3} when it issues a REST fetch, so the base URL
     * must not include a trailing slash or a path component.</p>
     *
     * @return scheme/host/port base URL such as {@code http://127.0.0.1:<port>}
     */
    String baseUrl() {
        return baseUrl;
    }

    /**
     * Replaces the REST response body returned by subsequent requests.
     *
     * <p>Recovery tests use this to change the snapshot sequence number between the initial
     * fetch and the post-recovery fetch so that the second snapshot aligns against a different
     * sequence boundary.</p>
     *
     * @param newResponseBody new JSON body to return for subsequent requests
     * @throws NullPointerException if {@code newResponseBody} is null
     */
    void setResponseBody(String newResponseBody) {
        if (newResponseBody == null) {
            throw new NullPointerException("newResponseBody");
        }
        this.responseBody.set(newResponseBody);
    }

    /**
     * Returns the cumulative number of REST requests received since construction.
     *
     * @return cumulative request count
     */
    int requestCount() {
        return requestCount.get();
    }

    /**
     * Stops the embedded {@link HttpServer} without waiting for in-flight requests.
     */
    @Override
    public void close() {
        server.stop(0);
    }
}
