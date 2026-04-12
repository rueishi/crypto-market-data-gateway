package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link MetricsEndpoint}.
 *
 * <p>The tests start the real JDK HTTP server on an OS-assigned local port and
 * use an in-memory Agrona {@link CountersManager} as the shared counter reader.
 * They cover the `/metrics` success path, automatic discovery of counters
 * allocated after endpoint startup, non-metrics path rejection, and idempotent
 * lifecycle methods without involving connector or parser hot paths.</p>
 */
class MetricsEndpointTest {

    /**
     * Verifies that `/metrics` serves Prometheus 0.0.4 text and discovers a
     * newly allocated counter on the next scrape without endpoint registration.
     *
     * @throws Exception if the HTTP client cannot scrape the local endpoint
     */
    @Test
    void servesMetricsAndDiscoversCountersAllocatedAfterStartup() throws Exception {
        CountersManager manager = GatewayCountersTest.newCountersManager();
        AtomicCounter first = manager.newCounter(
                "gateway_connection_attempts,instanceId=gateway-1,environment=test,venue=STUB");
        first.set(1);

        try (MetricsEndpoint endpoint = new MetricsEndpoint(manager, 0)) {
            endpoint.start();
            endpoint.start();

            HttpResponse<String> firstResponse = get(endpoint, "/metrics");
            assertThat(firstResponse.statusCode()).isEqualTo(200);
            assertThat(firstResponse.headers().firstValue("Content-Type")).contains(MetricsEndpoint.CONTENT_TYPE);
            assertThat(firstResponse.body()).contains("gateway_connection_attempts");

            AtomicCounter second = manager.newCounter(
                    "gateway_dynamic_counter,instanceId=gateway-1,environment=test,venue=STUB");
            second.set(7);

            HttpResponse<String> secondResponse = get(endpoint, "/metrics");
            assertThat(secondResponse.body())
                    .contains("gateway_dynamic_counter{instance_id=\"gateway-1\",environment=\"test\",venue=\"STUB\"} 7");

            endpoint.stop();
            endpoint.stop();
        }
    }

    /**
     * Verifies that only the exact `/metrics` scrape path succeeds.
     *
     * @throws Exception if the HTTP client cannot call the local endpoint
     */
    @Test
    void returnsNotFoundForNonMetricsPath() throws Exception {
        try (MetricsEndpoint endpoint = new MetricsEndpoint(GatewayCountersTest.newCountersManager(), 0)) {
            endpoint.start();

            HttpResponse<String> response = get(endpoint, "/metrics/extra");

            assertThat(response.statusCode()).isEqualTo(404);
        }
    }

    private static HttpResponse<String> get(MetricsEndpoint endpoint, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + endpoint.boundPort() + path))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
}
