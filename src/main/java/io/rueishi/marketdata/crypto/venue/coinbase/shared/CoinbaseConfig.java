package io.rueishi.marketdata.crypto.venue.coinbase.shared;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable typed configuration for Coinbase venue components.
 *
 * <p>{@code CoinbaseConfig} is the Phase 2 boundary between the raw
 * venue-specific map carried by {@code GatewayConfig} and the Coinbase
 * subscription/authentication classes that need validated values. The
 * Coinbase connector factory parses the raw {@code coinbase:} YAML block into
 * this record once during startup, then passes it to shared Coinbase helpers
 * such as {@link CoinbaseAuthenticator} and later L2 subscription builders.</p>
 *
 * <p>The record validates required credentials without including secret values
 * in exception messages or {@link #toString()}. Channels default to the Phase 2
 * Coinbase L2 control-plane set, {@code level2} and {@code heartbeat}, so tests
 * and later connector construction have deterministic behavior when the raw
 * block omits optional channel configuration.</p>
 *
 * @param endpoint Coinbase Exchange Direct Feed WebSocket endpoint
 * @param apiKey Coinbase API key used in authenticated control-plane payloads
 * @param apiSecret Coinbase API secret used for HMAC signing
 * @param apiPassphrase Coinbase API passphrase used in authenticated payloads
 * @param channels configured Coinbase channels, defensively copied
 */
public record CoinbaseConfig(
        URI endpoint,
        String apiKey,
        String apiSecret,
        String apiPassphrase,
        List<String> channels) {
    /** Default Coinbase Exchange Direct Feed endpoint used when config omits the endpoint. */
    public static final URI DEFAULT_ENDPOINT = URI.create("wss://ws-direct.exchange.coinbase.com");
    /** Required Phase 2 Coinbase L2 channels. */
    public static final List<String> DEFAULT_CHANNELS = List.of("level2", "heartbeat");

    /**
     * Creates an immutable validated Coinbase configuration.
     *
     * <p>The constructor is also used by tests that build configuration
     * directly. It rejects missing credentials, validates the WebSocket scheme,
     * and copies the channels list so later caller mutations cannot change
     * connector startup behavior.</p>
     *
     * @throws NullPointerException if {@code endpoint} or {@code channels} is null
     * @throws IllegalArgumentException if required text is blank, the endpoint is not WebSocket-based,
     *                                  or a channel name is blank
     */
    public CoinbaseConfig {
        Objects.requireNonNull(endpoint, "endpoint");
        apiKey = requireText(apiKey, "apiKey");
        apiSecret = requireText(apiSecret, "apiSecret");
        apiPassphrase = requireText(apiPassphrase, "apiPassphrase");
        validateWebSocketEndpoint(endpoint);
        channels = copyChannels(channels);
    }

    /**
     * Parses a Coinbase configuration from the raw venue config map.
     *
     * <p>{@code GatewayConfig#venueConfig} stores values from the raw YAML
     * {@code coinbase:} block after environment-variable placeholders have been
     * resolved. Later connector factories call this method once per startup to
     * translate that raw map into typed fields and explicit defaults. The
     * accepted credential keys are {@code apiKey}, {@code apiSecret}, and either
     * {@code passphrase} or {@code apiPassphrase}.</p>
     *
     * @param raw raw venue-specific configuration block
     * @return validated typed Coinbase configuration
     * @throws NullPointerException if {@code raw} is null
     * @throws IllegalArgumentException if a required value is missing or has the wrong type
     */
    public static CoinbaseConfig from(Map<String, Object> raw) {
        Objects.requireNonNull(raw, "raw");
        URI endpoint = endpoint(raw.get("endpoint"));
        String apiKey = text(raw.get("apiKey"), "apiKey");
        String apiSecret = text(raw.get("apiSecret"), "apiSecret");
        String apiPassphrase = passphrase(raw);
        List<String> channels = channels(raw.get("channels"));
        return new CoinbaseConfig(endpoint, apiKey, apiSecret, apiPassphrase, channels);
    }

    /**
     * Returns a redacted representation suitable for diagnostics.
     *
     * <p>Control-plane construction needs direct access to the credential
     * accessors, but diagnostic output must not leak configured secrets.</p>
     *
     * @return redacted configuration summary
     */
    @Override
    public String toString() {
        return "CoinbaseConfig[endpoint=" + endpoint
                + ", apiKey=<redacted>"
                + ", apiSecret=<redacted>"
                + ", apiPassphrase=<redacted>"
                + ", channels=" + channels + "]";
    }

    private static URI endpoint(Object value) {
        if (value == null) {
            return DEFAULT_ENDPOINT;
        }
        String text = text(value, "endpoint");
        try {
            return new URI(text);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("endpoint must be a valid URI", ex);
        }
    }

    private static String passphrase(Map<String, Object> raw) {
        Object value = raw.containsKey("apiPassphrase") ? raw.get("apiPassphrase") : raw.get("passphrase");
        return text(value, "passphrase");
    }

    private static List<String> channels(Object value) {
        if (value == null) {
            return DEFAULT_CHANNELS;
        }
        if (!(value instanceof List<?> rawChannels)) {
            throw new IllegalArgumentException("channels must be a list");
        }
        return rawChannels.stream()
                .map(channel -> {
                    if (!(channel instanceof String text)) {
                        throw new IllegalArgumentException("channels must contain only strings");
                    }
                    return text;
                })
                .toList();
    }

    private static List<String> copyChannels(List<String> channels) {
        Objects.requireNonNull(channels, "channels");
        if (channels.isEmpty()) {
            throw new IllegalArgumentException("channels must not be empty");
        }
        for (int i = 0; i < channels.size(); i++) {
            requireText(channels.get(i), "channels[" + i + "]");
        }
        return List.copyOf(channels);
    }

    private static String text(Object value, String name) {
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException(name + " must be a string");
        }
        return requireText(text, name);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static void validateWebSocketEndpoint(URI endpoint) {
        String scheme = endpoint.getScheme();
        if (!"wss".equalsIgnoreCase(scheme) && !"ws".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("endpoint must use ws or wss scheme");
        }
    }
}
