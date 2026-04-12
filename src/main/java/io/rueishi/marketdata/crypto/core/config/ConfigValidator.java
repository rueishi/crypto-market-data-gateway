package io.rueishi.marketdata.crypto.core.config;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Startup validator for {@link GatewayConfig}.
 *
 * <p>{@code ConfigValidator} is called by {@link ConfigLoader} after YAML has
 * been bound to Java objects and the venue-specific block has been extracted.
 * It centralizes startup failure rules for venue presence, instrument
 * uniqueness, publisher selection, and credential placeholder safety so
 * bootstrap can consume a validated object graph without repeating checks.</p>
 */
public final class ConfigValidator {
    private static final int MAX_LEVELS_PER_MESSAGE = 10_000;

    private ConfigValidator() {
    }

    /**
     * Validates a gateway configuration using {@link System#getenv(String)} for credential resolution.
     *
     * @param config configuration object to validate
     * @return the same validated configuration instance for fluent startup use
     * @throws NullPointerException if {@code config} is null
     * @throws IllegalArgumentException if validation fails
     */
    public static GatewayConfig validate(GatewayConfig config) {
        return validate(config, System::getenv);
    }

    /**
     * Validates a gateway configuration with an explicit environment resolver.
     *
     * <p>The resolver overload exists so tests and future bootstrap code can
     * verify credential behavior deterministically. Environment placeholder
     * values are replaced in {@link GatewayConfig#venueConfig} when they resolve
     * successfully; failures mention only the config path or variable name, not
     * the credential value.</p>
     *
     * @param config configuration object to validate
     * @param environmentResolver resolver used for {@code ${ENV_VAR}} credential placeholders
     * @return the same validated configuration instance for fluent startup use
     * @throws NullPointerException if {@code config} or {@code environmentResolver} is null
     * @throws IllegalArgumentException if validation fails
     */
    public static GatewayConfig validate(GatewayConfig config, Function<String, String> environmentResolver) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(environmentResolver, "environmentResolver");

        requireText(config.instanceId, "instanceId");
        requireText(config.environment, "environment");
        if (config.venue == null) {
            throw new IllegalArgumentException("venue is required. Valid values: " + VenueEnum.validValues());
        }
        validateInstruments(config);
        validateTransport(config.transport);
        validateEncoding(config.encoding);
        validateObservability(config.observability);
        validatePublisher(config.publisher);
        validateVenueConfig(config, environmentResolver);
        return config;
    }

    /**
     * Validates that the configured publisher type is recognized and has required per-type settings.
     *
     * @param publisher publisher configuration block
     * @return the parsed publisher type
     * @throws IllegalArgumentException if publisher configuration is missing or invalid
     */
    public static PublisherType validatePublisher(PublisherConfig publisher) {
        if (publisher == null || publisher.type == null || publisher.type.isBlank()) {
            throw new IllegalArgumentException(
                    "publisher.type is required. Valid values: " + publisherTypes());
        }

        PublisherType type;
        try {
            type = PublisherType.valueOf(publisher.type.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    "Unknown publisher.type '" + publisher.type + "'. Valid values: " + publisherTypes(), ex);
        }

        if (type == PublisherType.LOGGING) {
            if (publisher.logging == null) {
                throw new IllegalArgumentException("publisher.logging is required when publisher.type is LOGGING");
            }
            requireText(publisher.logging.outputPath, "publisher.logging.outputPath");
            if (publisher.logging.rollSizeMb <= 0) {
                throw new IllegalArgumentException("publisher.logging.rollSizeMb must be positive");
            }
        }
        return type;
    }

    private static void validateInstruments(GatewayConfig config) {
        if (config.instruments == null || config.instruments.isEmpty()) {
            throw new IllegalArgumentException("instruments must not be empty");
        }

        Set<String> exchangeSymbols = new HashSet<>();
        Set<Integer> instrumentIds = new HashSet<>();
        for (int i = 0; i < config.instruments.size(); i++) {
            InstrumentConfig instrument = config.instruments.get(i);
            if (instrument == null) {
                throw new IllegalArgumentException("instruments[" + i + "] must not be null");
            }
            String symbol = requireText(instrument.exchangeSymbol, "instruments[" + i + "].exchangeSymbol");
            if (!exchangeSymbols.add(symbol)) {
                throw new IllegalArgumentException("Duplicate instrument exchangeSymbol: " + symbol);
            }
            if (instrument.instrumentId <= 0) {
                throw new IllegalArgumentException("instruments[" + i + "].instrumentId must be positive");
            }
            if (!instrumentIds.add(instrument.instrumentId)) {
                throw new IllegalArgumentException("Duplicate instrument instrumentId: " + instrument.instrumentId);
            }
        }
    }

    private static void validateEncoding(EncodingConfig encoding) {
        if (encoding == null) {
            throw new IllegalArgumentException("encoding is required");
        }
        if (encoding.maxLevelsPerMessage <= 0 || encoding.maxLevelsPerMessage > MAX_LEVELS_PER_MESSAGE) {
            throw new IllegalArgumentException(
                    "encoding.maxLevelsPerMessage must be between 1 and " + MAX_LEVELS_PER_MESSAGE);
        }
        if (encoding.bufferHeadroomBytes < 0) {
            throw new IllegalArgumentException("encoding.bufferHeadroomBytes must not be negative");
        }
    }

    private static void validateTransport(TransportConfig transport) {
        if (transport == null) {
            throw new IllegalArgumentException("transport is required");
        }
        if (transport.connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("transport.connectTimeoutMs must be positive");
        }
        if (transport.reconnectBackoffBaseMs <= 0) {
            throw new IllegalArgumentException("transport.reconnectBackoffBaseMs must be positive");
        }
        if (transport.reconnectBackoffMaxMs < transport.reconnectBackoffBaseMs) {
            throw new IllegalArgumentException("transport.reconnectBackoffMaxMs must be >= reconnectBackoffBaseMs");
        }
        if (transport.reconnectBackoffJitterMs < 0) {
            throw new IllegalArgumentException("transport.reconnectBackoffJitterMs must not be negative");
        }
        if (transport.heartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException("transport.heartbeatTimeoutMs must be positive");
        }
        if (transport.frameSizeLimitBytes <= 0) {
            throw new IllegalArgumentException("transport.frameSizeLimitBytes must be positive");
        }
        if (transport.shutdownDeadlineMs <= 0) {
            throw new IllegalArgumentException("transport.shutdownDeadlineMs must be positive");
        }
        validateCpuAffinity(transport);
    }

    private static void validateCpuAffinity(TransportConfig transport) {
        if (transport.cpuAffinity == null) {
            transport.cpuAffinity = new CpuAffinityConfig();
        }
        CpuAffinityConfig affinity = transport.cpuAffinity;
        if (affinity.mode == null) {
            throw new IllegalArgumentException("transport.cpuAffinity.mode is required");
        }
        if (transport.busyWaitEnabled && !affinity.enabled) {
            throw new IllegalArgumentException("transport.busyWaitEnabled requires transport.cpuAffinity.enabled=true");
        }
        if (affinity.eventLoopCpuIds == null) {
            affinity.eventLoopCpuIds = java.util.List.of();
        }
        if (affinity.mode == CpuAffinityMode.EXPLICIT) {
            if (!affinity.enabled) {
                return;
            }
            if (affinity.eventLoopCpuIds.isEmpty()) {
                throw new IllegalArgumentException("transport.cpuAffinity.eventLoopCpuIds must not be empty for EXPLICIT mode");
            }
            Set<Integer> seen = new HashSet<>();
            for (Integer cpuId : affinity.eventLoopCpuIds) {
                if (cpuId == null || cpuId < 0) {
                    throw new IllegalArgumentException("transport.cpuAffinity.eventLoopCpuIds must be non-negative");
                }
                if (!seen.add(cpuId)) {
                    throw new IllegalArgumentException("transport.cpuAffinity.eventLoopCpuIds must be unique");
                }
            }
        }
    }

    /**
     * Validates startup paths for the Phase 3 shared observability runtime.
     *
     * <p>The runtime maps counter metadata/value files from
     * {@link ObservabilityConfig#countersSharedMemoryPath} and maps
     * {@link ObservabilityConfig#errorLogPath} directly for the distinct error
     * log. Validation stays lexical here so startup can fail early for missing
     * or blank config while filesystem permission and sizing failures remain
     * explicit runtime allocation errors.</p>
     *
     * @param observability observability configuration block
     * @throws IllegalArgumentException if the block, required paths, or metrics port are invalid
     */
    private static void validateObservability(ObservabilityConfig observability) {
        if (observability == null) {
            throw new IllegalArgumentException("observability is required");
        }
        requireText(observability.countersSharedMemoryPath, "observability.countersSharedMemoryPath");
        requireText(observability.errorLogPath, "observability.errorLogPath");
        if (observability.metricsHttpPort < 0 || observability.metricsHttpPort > 65_535) {
            throw new IllegalArgumentException("observability.metricsHttpPort must be between 0 and 65535");
        }
    }

    private static void validateVenueConfig(
            GatewayConfig config,
            Function<String, String> environmentResolver) {
        if (config.venueConfig == null) {
            throw new IllegalArgumentException(
                    "Missing venue config block '" + config.venue.venueConfigKey() + "' in gateway.yaml");
        }
        resolveCredentialPlaceholders(config.venueConfig, config.venue.venueConfigKey(), environmentResolver);
    }

    @SuppressWarnings("unchecked")
    private static void resolveCredentialPlaceholders(
            Map<String, Object> map,
            String path,
            Function<String, String> environmentResolver) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String childPath = path + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                resolveCredentialPlaceholders((Map<String, Object>) nested, childPath, environmentResolver);
            } else if (value instanceof String text) {
                String resolved = resolvePlaceholder(text, childPath, environmentResolver);
                if (isCredentialKey(entry.getKey()) && resolved.isBlank()) {
                    throw new IllegalArgumentException("Credential config '" + childPath + "' resolved to empty value");
                }
                entry.setValue(resolved);
            }
        }
    }

    private static String resolvePlaceholder(
            String value,
            String path,
            Function<String, String> environmentResolver) {
        if (value.startsWith("${") && value.endsWith("}") && value.length() > 3) {
            String variableName = value.substring(2, value.length() - 1);
            String resolved = environmentResolver.apply(variableName);
            if (resolved == null || resolved.isBlank()) {
                throw new IllegalArgumentException(
                        "Environment variable '" + variableName + "' for config '" + path
                                + "' resolved to empty value");
            }
            return resolved;
        }
        return value;
    }

    private static boolean isCredentialKey(String key) {
        String normalized = key.toLowerCase(Locale.ROOT);
        return normalized.contains("apikey")
                || normalized.contains("api_key")
                || normalized.contains("secret")
                || normalized.contains("passphrase")
                || normalized.contains("credential")
                || normalized.contains("password")
                || normalized.contains("token");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static String publisherTypes() {
        StringBuilder builder = new StringBuilder();
        for (PublisherType type : PublisherType.values()) {
            if (!builder.isEmpty()) {
                builder.append(", ");
            }
            builder.append(type.name());
        }
        return builder.toString();
    }
}
