package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PrometheusCounterFormatter}.
 *
 * <p>The suite exercises the formatter used by {@link MetricsEndpoint} without
 * starting an HTTP server. It uses an in-memory Agrona {@link CountersManager}
 * from the existing counter tests and covers gateway label parsing, Prometheus
 * label-name normalization, metric-name sanitization, and label-value escaping
 * for malformed or unusual counter labels.</p>
 */
class PrometheusCounterFormatterTest {

    /**
     * Verifies that gateway counter labels are converted into Prometheus sample
     * lines with normalized label names and the latest counter value.
     */
    @Test
    void formatsGatewayCounterLabelsAsPrometheusSamples() {
        CountersManager manager = GatewayCountersTest.newCountersManager();
        AtomicCounter counter = manager.newCounter(
                "gateway_connection_attempts,instanceId=gateway-1,environment=test,venue=COINBASE_L2");
        counter.set(3);

        String output = new PrometheusCounterFormatter().format(manager);

        assertThat(output).contains(
                "gateway_connection_attempts{instance_id=\"gateway-1\",environment=\"test\",venue=\"COINBASE_L2\"} 3\n");
    }

    /**
     * Verifies that unusual labels cannot break Prometheus rendering: metric
     * and label names are sanitized and label values are escaped.
     */
    @Test
    void sanitizesMetricAndLabelNamesAndEscapesValues() {
        CountersManager manager = GatewayCountersTest.newCountersManager();
        AtomicCounter counter = manager.newCounter(
                "9bad metric,bad key=value \"quoted\" \\\\ slash\nnewline,9label=value");
        counter.set(9);

        String output = new PrometheusCounterFormatter().format(manager);

        assertThat(output).contains(
                "_9bad_metric{bad_key=\"value \\\"quoted\\\" \\\\\\\\ slash\\nnewline\",_9label=\"value\"} 9\n");
    }

    /**
     * Verifies that a malformed label without a useful metric name still emits
     * a deterministic fallback sample instead of failing the scrape.
     */
    @Test
    void usesFallbackMetricNameForBlankLabels() {
        CountersManager manager = GatewayCountersTest.newCountersManager();
        AtomicCounter counter = manager.newCounter(" ");
        counter.set(5);

        String output = new PrometheusCounterFormatter().format(manager);

        assertThat(output).contains("gateway_counter_" + counter.id() + " 5\n");
    }
}
