package io.rueishi.marketdata.crypto.venue.coinbase.shared;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoinbaseConfig}.
 *
 * <p>The suite covers parsing the raw Coinbase venue config map that later
 * connector factories receive from {@code GatewayConfig#venueConfig}. It uses
 * simple maps rather than mocking the full bootstrap path so P2-000 can verify
 * typed parsing, defaults, immutability, and credential redaction in isolation
 * from the later Coinbase L2 connector cards.</p>
 */
class CoinbaseConfigTest {

    /**
     * Verifies that a complete raw Coinbase config map becomes a typed immutable config.
     */
    @Test
    void parsesValidRawVenueConfig() {
        CoinbaseConfig config = CoinbaseConfig.from(validRawConfig());

        assertThat(config.endpoint()).isEqualTo(URI.create("wss://ws-feed.exchange.coinbase.com"));
        assertThat(config.apiKey()).isEqualTo("key-1");
        assertThat(config.apiSecret()).isEqualTo("c2VjcmV0");
        assertThat(config.apiPassphrase()).isEqualTo("passphrase-1");
        assertThat(config.channels()).containsExactly("level2", "heartbeat");
    }

    /**
     * Verifies explicit defaults for optional endpoint and channels.
     */
    @Test
    void defaultsEndpointAndChannelsWhenOptionalFieldsAreAbsent() {
        Map<String, Object> raw = validRawConfig();
        raw.remove("endpoint");
        raw.remove("channels");

        CoinbaseConfig config = CoinbaseConfig.from(raw);

        assertThat(config.endpoint()).isEqualTo(CoinbaseConfig.DEFAULT_ENDPOINT);
        assertThat(config.channels()).containsExactly("level2", "heartbeat");
    }

    /**
     * Verifies that later caller mutations cannot change parsed channel configuration.
     */
    @Test
    void defensivelyCopiesChannels() {
        List<String> channels = new ArrayList<>(List.of("level2", "heartbeat"));
        Map<String, Object> raw = validRawConfig();
        raw.put("channels", channels);

        CoinbaseConfig config = CoinbaseConfig.from(raw);
        channels.add("ticker");

        assertThat(config.channels()).containsExactly("level2", "heartbeat");
        assertThatThrownBy(() -> config.channels().add("ticker"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * Verifies invalid config fails clearly without including secret values in diagnostics.
     */
    @Test
    void rejectsInvalidConfigWithoutSecretLeakage() {
        Map<String, Object> raw = validRawConfig();
        raw.put("apiSecret", "SUPER_SECRET_SHOULD_NOT_APPEAR");
        raw.put("passphrase", "");

        assertThatThrownBy(() -> CoinbaseConfig.from(raw))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("passphrase")
                .hasMessageNotContaining("SUPER_SECRET_SHOULD_NOT_APPEAR");
    }

    /**
     * Verifies diagnostic output redacts configured credentials.
     */
    @Test
    void toStringRedactsCredentialValues() {
        CoinbaseConfig config = CoinbaseConfig.from(validRawConfig());

        assertThat(config.toString())
                .contains("<redacted>")
                .doesNotContain("key-1")
                .doesNotContain("c2VjcmV0")
                .doesNotContain("passphrase-1");
    }

    private static Map<String, Object> validRawConfig() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("endpoint", "wss://ws-feed.exchange.coinbase.com");
        raw.put("apiKey", "key-1");
        raw.put("apiSecret", "c2VjcmV0");
        raw.put("passphrase", "passphrase-1");
        raw.put("channels", List.of("level2", "heartbeat"));
        return raw;
    }
}
