package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;

/**
 * Coinbase L3 authenticated WebSocket subscription payload builder.
 *
 * <p>{@code CoinbaseL3SubscriptionBuilder} is the venue-specific
 * {@link SubscriptionBuilder} implementation for the Coinbase full-order feed.
 * Connector factory code creates one builder from typed
 * {@link CoinbaseConfig}, a shared {@link CoinbaseAuthenticator}, and an epoch
 * clock; later connector lifecycle code calls
 * {@link #buildSubscribe(InstrumentConfig)} when the WebSocket session is ready
 * and {@link #buildUnsubscribe(InstrumentConfig)} during shutdown or recovery
 * teardown.</p>
 *
 * <p>Each call produces one UTF-8 JSON control-plane payload containing the
 * configured instrument product id, both required L3 channels
 * ({@code full}/{@code heartbeat}), and fresh Coinbase authentication fields
 * generated from the shared authenticator. The builder does not send frames or
 * manage session state; it only serializes deterministic JSON for the
 * connector transport path.</p>
 */
public final class CoinbaseL3SubscriptionBuilder implements SubscriptionBuilder {
    /** Coinbase L3 full-order channel required by Phase 4. */
    public static final String FULL_CHANNEL = "full";
    /** Coinbase heartbeat channel required by Phase 4 liveness handling. */
    public static final String HEARTBEAT_CHANNEL = "heartbeat";

    private final CoinbaseAuthenticator authenticator;
    private final EpochClock epochClock;
    private final List<String> channels;

    /**
     * Creates a builder with the system epoch clock.
     *
     * <p>Production connector factories can use this constructor when they do
     * not need deterministic timestamp control. Tests generally prefer the
     * explicit clock constructor.</p>
     *
     * @param config typed Coinbase venue config
     * @throws NullPointerException if {@code config} is null
     * @throws IllegalArgumentException if credentials or required channels are invalid
     */
    public CoinbaseL3SubscriptionBuilder(CoinbaseConfig config) {
        this(config, new CoinbaseAuthenticator(config), SystemEpochClock.INSTANCE);
    }

    /**
     * Creates a builder with explicit authentication and clock dependencies.
     *
     * <p>This constructor is used by tests and later connector factories that
     * already own a shared authenticator. It validates that the configured
     * channel list contains both required L3 channels before any subscribe or
     * unsubscribe payload can be emitted.</p>
     *
     * @param config typed Coinbase venue config containing channel selection
     * @param authenticator shared Coinbase authentication helper
     * @param epochClock injectable clock used for auth timestamp generation
     * @throws NullPointerException if any dependency is null
     * @throws IllegalArgumentException if the channel list omits a required channel
     */
    public CoinbaseL3SubscriptionBuilder(
            CoinbaseConfig config,
            CoinbaseAuthenticator authenticator,
            EpochClock epochClock) {
        Objects.requireNonNull(config, "config");
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
        this.epochClock = Objects.requireNonNull(epochClock, "epochClock");
        this.channels = requiredChannels(config.channels());
    }

    /**
     * Builds one authenticated Coinbase subscribe payload for the full feed.
     *
     * <p>The payload contains the instrument's {@code exchangeSymbol}, the
     * required L3 channels, and fresh auth fields using the current epoch
     * second.</p>
     *
     * @param instrument configured instrument to subscribe
     * @return UTF-8 encoded Coinbase subscribe JSON payload
     * @throws NullPointerException if {@code instrument} is null
     * @throws IllegalArgumentException if {@code instrument.exchangeSymbol} is blank
     */
    @Override
    public byte[] buildSubscribe(InstrumentConfig instrument) {
        return buildPayload("subscribe", instrument);
    }

    /**
     * Builds one authenticated Coinbase unsubscribe payload for the full feed.
     *
     * <p>The unsubscribe mirrors the subscribe control-plane scope so recovery
     * or shutdown can remove exactly the L3 subscription previously installed.</p>
     *
     * @param instrument configured instrument to unsubscribe
     * @return UTF-8 encoded Coinbase unsubscribe JSON payload
     * @throws NullPointerException if {@code instrument} is null
     * @throws IllegalArgumentException if {@code instrument.exchangeSymbol} is blank
     */
    @Override
    public byte[] buildUnsubscribe(InstrumentConfig instrument) {
        return buildPayload("unsubscribe", instrument);
    }

    /**
     * Serializes a Coinbase L3 control-plane payload with stable field order.
     *
     * @param type Coinbase control-plane type, either subscribe or unsubscribe
     * @param instrument configured instrument whose exchange symbol becomes product id
     * @return UTF-8 JSON payload bytes
     */
    private byte[] buildPayload(String type, InstrumentConfig instrument) {
        Objects.requireNonNull(instrument, "instrument");
        String productId = requireText(instrument.exchangeSymbol, "instrument.exchangeSymbol");
        CoinbaseAuthenticator.AuthFields authFields = authenticator.authFields(epochClock);

        StringBuilder builder = new StringBuilder(256);
        builder.append('{');
        appendField(builder, "type", type).append(',');
        appendArrayField(builder, "product_ids", List.of(productId)).append(',');
        appendArrayField(builder, "channels", channels).append(',');
        appendField(builder, "signature", authFields.signature()).append(',');
        appendField(builder, "key", authFields.key()).append(',');
        appendField(builder, "passphrase", authFields.passphrase()).append(',');
        appendField(builder, "timestamp", authFields.timestamp());
        builder.append('}');
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Validates and de-duplicates the configured L3 channel set.
     *
     * @param configuredChannels raw Coinbase channel list from config
     * @return immutable de-duplicated channel list in configured order
     * @throws NullPointerException if {@code configuredChannels} is null
     * @throws IllegalArgumentException if a channel is blank or a required L3 channel is missing
     */
    private static List<String> requiredChannels(List<String> configuredChannels) {
        Objects.requireNonNull(configuredChannels, "configuredChannels");
        List<String> result = new ArrayList<>(configuredChannels.size());
        for (String channel : configuredChannels) {
            String text = requireText(channel, "channel");
            if (!result.contains(text)) {
                result.add(text);
            }
        }
        if (!result.contains(FULL_CHANNEL) || !result.contains(HEARTBEAT_CHANNEL)) {
            throw new IllegalArgumentException("Coinbase L3 subscriptions require full and heartbeat channels");
        }
        return List.copyOf(result);
    }

    /**
     * Appends one quoted JSON string field to the payload under construction.
     *
     * @param builder destination JSON builder
     * @param name JSON field name
     * @param value JSON string value
     * @return the same builder for fluent chaining
     */
    private static StringBuilder appendField(StringBuilder builder, String name, String value) {
        return builder.append('"')
                .append(name)
                .append("\":\"")
                .append(escapeJson(value))
                .append('"');
    }

    /**
     * Appends one JSON string array field in stable iteration order.
     *
     * @param builder destination JSON builder
     * @param name JSON field name
     * @param values JSON string values to append
     * @return the same builder for fluent chaining
     */
    private static StringBuilder appendArrayField(StringBuilder builder, String name, List<String> values) {
        builder.append('"').append(name).append("\":[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append('"').append(escapeJson(values.get(i))).append('"');
        }
        return builder.append(']');
    }

    /**
     * Escapes a Java string for safe inclusion as a JSON string value.
     *
     * @param value raw Java string
     * @return JSON-escaped representation
     */
    private static String escapeJson(String value) {
        StringBuilder escaped = null;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            String replacement = switch (current) {
                case '"' -> "\\\"";
                case '\\' -> "\\\\";
                case '\b' -> "\\b";
                case '\f' -> "\\f";
                case '\n' -> "\\n";
                case '\r' -> "\\r";
                case '\t' -> "\\t";
                default -> current < 0x20 ? unicodeEscape(current) : null;
            };
            if (replacement != null) {
                if (escaped == null) {
                    escaped = new StringBuilder(value.length() + 8);
                    escaped.append(value, 0, i);
                }
                escaped.append(replacement);
            } else if (escaped != null) {
                escaped.append(current);
            }
        }
        return escaped == null ? value : escaped.toString();
    }

    /**
     * Validates that required text input is present.
     *
     * @param value text value to validate
     * @param name field name used in the exception message
     * @return the original validated value
     * @throws IllegalArgumentException if the value is null or blank
     */
    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    /**
     * Encodes one control character as a four-digit JSON unicode escape.
     *
     * @param value control character to escape
     * @return unicode escape string such as {@code \\u000a}
     */
    private static String unicodeEscape(char value) {
        char[] escaped = {'\\', 'u', '0', '0', '0', '0'};
        int code = value;
        for (int i = 5; i >= 2; i--) {
            int nibble = code & 0xF;
            escaped[i] = (char) (nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));
            code >>>= 4;
        }
        return new String(escaped);
    }
}
