package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3 integration tests for mapped observability resources and metrics scraping.
 *
 * <p>The suite uses the real {@link ObservabilityRuntime}, memory-mapped
 * counter storage, {@link GatewayCounters}, and {@link MetricsEndpoint}. It
 * verifies that metrics scrape counters generically after startup and that
 * counter metadata/value files can be remapped after a process-style close,
 * matching the abnormal termination diagnostics contract without involving
 * venue connector code.</p>
 */
class Phase3ObservabilityIntegrationTest {

    /**
     * Verifies a metrics endpoint started before instrument counters are
     * allocated discovers those counters on the next scrape.
     *
     * @throws Exception if the local HTTP metrics scrape fails
     */
    @Test
    void metricsEndpointDiscoversInstrumentCountersAllocatedAfterStartup(@TempDir Path tempDir) throws Exception {
        ObservabilityConfig config = observabilityConfig(tempDir);
        try (ObservabilityRuntime runtime = ObservabilityRuntime.create(
                        config,
                        "gateway-phase3",
                        "test",
                        "COINBASE_L2");
                MetricsEndpoint endpoint = new MetricsEndpoint(runtime.countersReader(), 0, runtime.errorLog())) {
            endpoint.start();

            InstrumentCounters instrumentCounters = runtime.counters().forInstrument(1001);
            instrumentCounters.messagesDecoded().set(7);

            HttpResponse<String> response = get(endpoint);

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains(
                    "gateway_messages_decoded{instance_id=\"gateway-phase3\",environment=\"test\","
                            + "venue=\"COINBASE_L2\",instrument_id=\"1001\"} 7");
        }
    }

    /**
     * Verifies mapped counter values remain readable when the storage is closed
     * and later remapped, matching crash-restart inspection behavior.
     */
    @Test
    @SuppressWarnings("resource")
    void mappedCounterValuesRemainReadableAfterCloseAndRemap(@TempDir Path tempDir) {
        Path basePath = tempDir.resolve("phase3-counters");
        int counterId;

        try (MappedCountersFactory factory = MappedCountersFactory.create(basePath, 8)) {
            AtomicCounter counter = factory.countersManager().newCounter(
                    "gateway_phase3_remap,instanceId=gateway-phase3,environment=test,venue=COINBASE_L2");
            counterId = counter.id();
            counter.set(99);
        }

        try (MappedCountersFactory remapped = MappedCountersFactory.create(basePath, 8)) {
            assertThat(remapped.countersReader().getCounterLabel(counterId)).contains("gateway_phase3_remap");
            assertThat(remapped.countersReader().getCounterValue(counterId)).isEqualTo(99);
        }
    }

    private static ObservabilityConfig observabilityConfig(Path tempDir) {
        ObservabilityConfig config = new ObservabilityConfig();
        config.countersSharedMemoryPath = tempDir.resolve("counters").toString();
        config.errorLogPath = tempDir.resolve("errors").toString();
        config.metricsHttpPort = 0;
        return config;
    }

    private static HttpResponse<String> get(MetricsEndpoint endpoint) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + endpoint.boundPort() + "/metrics"))
                .GET()
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
}
