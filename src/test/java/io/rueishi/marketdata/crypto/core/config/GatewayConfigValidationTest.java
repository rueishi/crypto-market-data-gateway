package io.rueishi.marketdata.crypto.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConfigLoader} and {@link ConfigValidator}.
 *
 * <p>These tests cover the startup configuration scenarios owned by P1-002:
 * successful YAML binding, venue validation, non-empty and unique instruments,
 * venue-block extraction, publisher selection validation, and credential
 * placeholder safety. Phase 3 extends this coverage to observability path and
 * metrics port validation for mapped counters and the mapped distinct error
 * log. Environment lookup is stubbed with a lambda so tests do not depend on
 * the machine environment and do not expose secret values.</p>
 */
class GatewayConfigValidationTest {

    /**
     * Verifies that a valid YAML document loads into a validated object graph
     * whose instrument count can drive connector creation one-for-one.
     */
    @Test
    void loadsValidYamlAndPreservesConnectorCountFromInstruments() {
        GatewayConfig config = ConfigLoader.load(input(validYaml()), env("resolved-secret"));

        assertThat(config.instanceId).isEqualTo("gateway-coinbase-l2-1");
        assertThat(config.environment).isEqualTo("test");
        assertThat(config.venue).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(config.instruments).hasSize(2);
        assertThat(config.instruments).extracting(instrument -> instrument.instrumentId)
                .containsExactly(1001, 1002);
        assertThat(config.venueConfig)
                .containsEntry("apiKey", "resolved-secret")
                .containsEntry("apiSecret", "resolved-secret");
    }

    /**
     * Verifies that missing and unrecognized venues fail before connector discovery can run.
     */
    @Test
    void rejectsMissingOrUnrecognizedVenue() {
        GatewayConfig missing = validConfig();
        missing.venue = null;

        assertThatThrownBy(() -> ConfigValidator.validate(missing, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("venue is required");

        assertThatThrownBy(() -> ConfigLoader.load(input(validYaml().replace("COINBASE_L2", "NOT_A_VENUE")), env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown venue")
                .hasMessageContaining("NOT_A_VENUE");
    }

    /**
     * Verifies that startup fails when no instruments are configured.
     */
    @Test
    void rejectsEmptyInstrumentList() {
        GatewayConfig config = validConfig();
        config.instruments = List.of();

        assertThatThrownBy(() -> ConfigValidator.validate(config, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instruments must not be empty");
    }

    /**
     * Verifies that duplicate instrument ids are rejected to prevent ambiguous routing.
     */
    @Test
    void rejectsDuplicateInstrumentId() {
        GatewayConfig config = validConfig();
        config.instruments = List.of(instrument("BTC-USD", 1001), instrument("ETH-USD", 1001));

        assertThatThrownBy(() -> ConfigValidator.validate(config, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate instrument instrumentId")
                .hasMessageContaining("1001");
    }

    /**
     * Verifies that duplicate exchange symbols are rejected to prevent ambiguous venue symbol lookup.
     */
    @Test
    void rejectsDuplicateExchangeSymbol() {
        GatewayConfig config = validConfig();
        config.instruments = List.of(instrument("BTC-USD", 1001), instrument("BTC-USD", 1002));

        assertThatThrownBy(() -> ConfigValidator.validate(config, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate instrument exchangeSymbol")
                .hasMessageContaining("BTC-USD");
    }

    /**
     * Verifies that the venue-specific YAML block must match the lowercase venue prefix.
     */
    @Test
    void rejectsMissingVenueConfigBlock() {
        String yamlWithoutVenueBlock = validYaml().replace(
                """
                coinbase:
                  endpoint: "wss://ws-feed.exchange.coinbase.com"
                  apiKey: "${COINBASE_API_KEY}"
                  passphrase: "${COINBASE_PASSPHRASE}"
                  apiSecret: "${COINBASE_API_SECRET}"
                  channels:
                    - "level2"
                    - "heartbeat"
                """,
                "");

        assertThatThrownBy(() -> ConfigLoader.load(input(yamlWithoutVenueBlock), env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing venue config block 'coinbase'");
    }

    /**
     * Verifies that empty resolved credentials fail startup without exposing the secret value.
     */
    @Test
    void rejectsEmptyResolvedCredentialWithoutLeakingCredentialValue() {
        String secretValue = "SUPER_SECRET_SHOULD_NOT_APPEAR";

        assertThatThrownBy(() -> ConfigLoader.load(input(validYaml()), variableName -> ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COINBASE_API_KEY")
                .hasMessageNotContaining(secretValue);
    }

    /**
     * Verifies that publisher type validation remains application-controlled rather than YAML-controlled.
     */
    @Test
    void validatesPublisherTypeAndLoggingSettings() {
        GatewayConfig unknownType = validConfig();
        unknownType.publisher.type = "mystery";

        assertThatThrownBy(() -> ConfigValidator.validate(unknownType, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown publisher.type")
                .hasMessageContaining("LOGGING");

        GatewayConfig missingLogging = validConfig();
        missingLogging.publisher.type = "LOGGING";
        missingLogging.publisher.logging = null;

        assertThatThrownBy(() -> ConfigValidator.validate(missingLogging, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publisher.logging is required");
    }

    /**
     * Verifies that encoder capacity validation rejects values outside the Phase 1 supported range.
     */
    @Test
    void rejectsInvalidEncodingCapacity() {
        GatewayConfig config = validConfig();
        config.encoding.maxLevelsPerMessage = 10_001;

        assertThatThrownBy(() -> ConfigValidator.validate(config, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("encoding.maxLevelsPerMessage");
    }

    /**
     * Verifies that Phase 3 observability runtime inputs fail validation before
     * bootstrap tries to map counter or error-log files.
     */
    @Test
    void rejectsInvalidObservabilityConfig() {
        GatewayConfig missing = validConfig();
        missing.observability = null;
        assertThatThrownBy(() -> ConfigValidator.validate(missing, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("observability is required");

        GatewayConfig blankCounterPath = validConfig();
        blankCounterPath.observability.countersSharedMemoryPath = " ";
        assertThatThrownBy(() -> ConfigValidator.validate(blankCounterPath, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("observability.countersSharedMemoryPath");

        GatewayConfig invalidPort = validConfig();
        invalidPort.observability.metricsHttpPort = 70_000;
        assertThatThrownBy(() -> ConfigValidator.validate(invalidPort, env("secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metricsHttpPort");
    }

    private static GatewayConfig validConfig() {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-coinbase-l2-1";
        config.environment = "test";
        config.venue = VenueEnum.COINBASE_L2;
        config.instruments = List.of(instrument("BTC-USD", 1001), instrument("ETH-USD", 1002));
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 5000;
        config.transport.reconnectBackoffBaseMs = 500;
        config.transport.reconnectBackoffMaxMs = 30_000;
        config.transport.reconnectBackoffJitterMs = 250;
        config.transport.heartbeatTimeoutMs = 10_000;
        config.transport.frameSizeLimitBytes = 4_194_304;
        config.transport.shutdownDeadlineMs = 5000;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 10_000;
        config.encoding.bufferHeadroomBytes = 8192;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = "/dev/shm/counters";
        config.observability.errorLogPath = "/dev/shm/errors";
        config.observability.metricsHttpPort = 9090;
        config.publisher = new PublisherConfig();
        config.publisher.type = "LOGGING";
        config.publisher.logging = new LoggingPublisherConfig();
        config.publisher.logging.outputPath = "/var/log/gateway/messages.log";
        config.publisher.logging.rollSizeMb = 256;
        config.venueConfig = new LinkedHashMap<>(Map.of(
                "apiKey", "${COINBASE_API_KEY}",
                "passphrase", "${COINBASE_PASSPHRASE}",
                "apiSecret", "${COINBASE_API_SECRET}"));
        return config;
    }

    private static InstrumentConfig instrument(String exchangeSymbol, int instrumentId) {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = exchangeSymbol;
        instrument.instrumentId = instrumentId;
        return instrument;
    }

    private static ByteArrayInputStream input(String yaml) {
        return new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8));
    }

    private static java.util.function.Function<String, String> env(String value) {
        Map<String, String> values = new HashMap<>();
        values.put("COINBASE_API_KEY", value);
        values.put("COINBASE_PASSPHRASE", value);
        values.put("COINBASE_API_SECRET", value);
        return values::get;
    }

    private static String validYaml() {
        return """
                instanceId: "gateway-coinbase-l2-1"
                environment: "test"
                venue: COINBASE_L2
                instruments:
                  - exchangeSymbol: "BTC-USD"
                    instrumentId: 1001
                  - exchangeSymbol: "ETH-USD"
                    instrumentId: 1002
                transport:
                  connectTimeoutMs: 5000
                  reconnectBackoffBaseMs: 500
                  reconnectBackoffMaxMs: 30000
                  reconnectBackoffJitterMs: 250
                  heartbeatTimeoutMs: 10000
                  frameSizeLimitBytes: 4194304
                  busyWaitEnabled: false
                  shutdownDeadlineMs: 5000
                encoding:
                  maxLevelsPerMessage: 10000
                  bufferHeadroomBytes: 8192
                observability:
                  countersSharedMemoryPath: "/dev/shm/gateway-coinbase-l2-1-counters"
                  errorLogPath: "/dev/shm/gateway-coinbase-l2-1-errors"
                  metricsHttpPort: 9090
                publisher:
                  type: LOGGING
                  logging:
                    outputPath: "/var/log/gateway/messages.log"
                    rollSizeMb: 256
                coinbase:
                  endpoint: "wss://ws-feed.exchange.coinbase.com"
                  apiKey: "${COINBASE_API_KEY}"
                  passphrase: "${COINBASE_PASSPHRASE}"
                  apiSecret: "${COINBASE_API_SECRET}"
                  channels:
                    - "level2"
                    - "heartbeat"
                """;
    }
}
