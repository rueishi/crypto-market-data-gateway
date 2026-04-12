package io.rueishi.marketdata.crypto.core.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.yaml.snakeyaml.Yaml;

/**
 * Startup-only YAML loader for {@link GatewayConfig}.
 *
 * <p>{@code ConfigLoader} is used by bootstrap before connector construction.
 * It parses the flat gateway YAML schema into the public-field configuration
 * model, extracts the venue-specific block based on {@link VenueEnum}, resolves
 * credential placeholders through {@link ConfigValidator}, and returns a
 * validated config object. SnakeYAML usage is deliberately isolated here so no
 * parser or connector hot-path class imports YAML parsing code.</p>
 */
public final class ConfigLoader {
    private ConfigLoader() {
    }

    /**
     * Loads and validates gateway configuration from a filesystem path.
     *
     * @param path YAML file path
     * @return validated gateway configuration
     * @throws IOException if the file cannot be opened
     * @throws IllegalArgumentException if the YAML content is missing or invalid
     */
    public static GatewayConfig load(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        try (InputStream inputStream = Files.newInputStream(path)) {
            return load(inputStream);
        }
    }

    /**
     * Loads and validates gateway configuration from an input stream.
     *
     * @param inputStream YAML input stream owned by the caller
     * @return validated gateway configuration
     * @throws IllegalArgumentException if the YAML content is missing or invalid
     */
    public static GatewayConfig load(InputStream inputStream) {
        return load(inputStream, System::getenv);
    }

    /**
     * Loads and validates gateway configuration from an input stream with an explicit environment resolver.
     *
     * <p>The resolver overload is used by tests and can also be used by
     * bootstrap if it wants to control environment lookup. The stream is read
     * but not closed by this method.</p>
     *
     * @param inputStream YAML input stream owned by the caller
     * @param environmentResolver resolver for {@code ${ENV_VAR}} credential placeholders
     * @return validated gateway configuration
     * @throws NullPointerException if {@code inputStream} or {@code environmentResolver} is null
     * @throws IllegalArgumentException if YAML parsing, binding, or validation fails
     */
    public static GatewayConfig load(
            InputStream inputStream,
            Function<String, String> environmentResolver) {
        Objects.requireNonNull(inputStream, "inputStream");
        Objects.requireNonNull(environmentResolver, "environmentResolver");

        Object loaded = new Yaml().load(inputStream);
        if (!(loaded instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("gateway.yaml must contain a top-level map");
        }

        GatewayConfig config = bind(asStringObjectMap(raw, "gateway.yaml"));
        return ConfigValidator.validate(config, environmentResolver);
    }

    private static GatewayConfig bind(Map<String, Object> raw) {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = asString(raw.get("instanceId"), "instanceId");
        config.environment = asString(raw.get("environment"), "environment");
        config.venue = VenueEnum.parse(asString(raw.get("venue"), "venue"));
        config.instruments = bindInstruments(raw.get("instruments"));
        config.transport = bindTransport(raw.get("transport"));
        config.encoding = bindEncoding(raw.get("encoding"));
        config.observability = bindObservability(raw.get("observability"));
        config.publisher = bindPublisher(raw.get("publisher"));
        Object venueBlock = raw.get(config.venue.venueConfigKey());
        if (venueBlock == null) {
            throw new IllegalArgumentException(
                    "Missing venue config block '" + config.venue.venueConfigKey() + "' in gateway.yaml");
        }
        config.venueConfig = asStringObjectMap(venueBlock, config.venue.venueConfigKey());
        return config;
    }

    private static List<InstrumentConfig> bindInstruments(Object value) {
        if (!(value instanceof List<?> rawList)) {
            throw new IllegalArgumentException("instruments must be a list");
        }
        List<InstrumentConfig> instruments = new ArrayList<>(rawList.size());
        for (int i = 0; i < rawList.size(); i++) {
            Map<String, Object> raw = asStringObjectMap(rawList.get(i), "instruments[" + i + "]");
            InstrumentConfig instrument = new InstrumentConfig();
            instrument.exchangeSymbol = asString(raw.get("exchangeSymbol"), "instruments[" + i + "].exchangeSymbol");
            instrument.instrumentId = asInt(raw.get("instrumentId"), "instruments[" + i + "].instrumentId");
            instruments.add(instrument);
        }
        return instruments;
    }

    private static TransportConfig bindTransport(Object value) {
        Map<String, Object> raw = asStringObjectMap(value, "transport");
        TransportConfig config = new TransportConfig();
        config.connectTimeoutMs = asInt(raw.get("connectTimeoutMs"), "transport.connectTimeoutMs");
        config.reconnectBackoffBaseMs = asLong(raw.get("reconnectBackoffBaseMs"), "transport.reconnectBackoffBaseMs");
        config.reconnectBackoffMaxMs = asLong(raw.get("reconnectBackoffMaxMs"), "transport.reconnectBackoffMaxMs");
        config.reconnectBackoffJitterMs = asLong(raw.get("reconnectBackoffJitterMs"), "transport.reconnectBackoffJitterMs");
        config.heartbeatTimeoutMs = asLong(raw.get("heartbeatTimeoutMs"), "transport.heartbeatTimeoutMs");
        config.frameSizeLimitBytes = asInt(raw.get("frameSizeLimitBytes"), "transport.frameSizeLimitBytes");
        config.busyWaitEnabled = asBoolean(raw.get("busyWaitEnabled"), "transport.busyWaitEnabled");
        if (raw.get("cpuAffinity") != null) {
            config.cpuAffinity = bindCpuAffinity(raw.get("cpuAffinity"));
        }
        config.shutdownDeadlineMs = asLong(raw.get("shutdownDeadlineMs"), "transport.shutdownDeadlineMs");
        return config;
    }

    private static CpuAffinityConfig bindCpuAffinity(Object value) {
        Map<String, Object> raw = asStringObjectMap(value, "transport.cpuAffinity");
        CpuAffinityConfig config = new CpuAffinityConfig();
        config.enabled = asBoolean(raw.get("enabled"), "transport.cpuAffinity.enabled");
        Object mode = raw.get("mode");
        if (mode != null) {
            try {
                config.mode = CpuAffinityMode.valueOf(asString(mode, "transport.cpuAffinity.mode").toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("transport.cpuAffinity.mode must be AUTO or EXPLICIT", ex);
            }
        }
        Object cpuIds = raw.get("eventLoopCpuIds");
        if (cpuIds != null) {
            if (!(cpuIds instanceof List<?> list)) {
                throw new IllegalArgumentException("transport.cpuAffinity.eventLoopCpuIds must be a list");
            }
            List<Integer> parsed = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                parsed.add(asInt(list.get(i), "transport.cpuAffinity.eventLoopCpuIds[" + i + "]"));
            }
            config.eventLoopCpuIds = List.copyOf(parsed);
        }
        return config;
    }

    private static EncodingConfig bindEncoding(Object value) {
        Map<String, Object> raw = asStringObjectMap(value, "encoding");
        EncodingConfig config = new EncodingConfig();
        config.maxLevelsPerMessage = asInt(raw.get("maxLevelsPerMessage"), "encoding.maxLevelsPerMessage");
        config.bufferHeadroomBytes = asInt(raw.get("bufferHeadroomBytes"), "encoding.bufferHeadroomBytes");
        return config;
    }

    private static ObservabilityConfig bindObservability(Object value) {
        Map<String, Object> raw = asStringObjectMap(value, "observability");
        ObservabilityConfig config = new ObservabilityConfig();
        config.countersSharedMemoryPath =
                asString(raw.get("countersSharedMemoryPath"), "observability.countersSharedMemoryPath");
        config.errorLogPath = asString(raw.get("errorLogPath"), "observability.errorLogPath");
        config.metricsHttpPort = asInt(raw.get("metricsHttpPort"), "observability.metricsHttpPort");
        return config;
    }

    private static PublisherConfig bindPublisher(Object value) {
        Map<String, Object> raw = asStringObjectMap(value, "publisher");
        PublisherConfig config = new PublisherConfig();
        config.type = asString(raw.get("type"), "publisher.type");
        if (raw.get("logging") != null) {
            Map<String, Object> loggingRaw = asStringObjectMap(raw.get("logging"), "publisher.logging");
            LoggingPublisherConfig logging = new LoggingPublisherConfig();
            logging.outputPath = asString(loggingRaw.get("outputPath"), "publisher.logging.outputPath");
            logging.rollSizeMb = asInt(loggingRaw.get("rollSizeMb"), "publisher.logging.rollSizeMb");
            config.logging = logging;
        }
        return config;
    }

    private static Map<String, Object> asStringObjectMap(Object value, String name) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(name + " must be a map");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException(name + " contains a non-string key");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private static String asString(Object value, String name) {
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException(name + " must be a string");
        }
        return text;
    }

    private static int asInt(Object value, String name) {
        if (value instanceof Integer number) {
            return number;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException(name + " must be an integer");
    }

    private static long asLong(Object value, String name) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException(name + " must be an integer");
    }

    private static boolean asBoolean(Object value, String name) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException(name + " must be a boolean");
    }
}
