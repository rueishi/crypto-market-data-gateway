package io.rueishi.marketdata.crypto.venue.coinbase.shared;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoinbaseAuthenticator}.
 *
 * <p>The tests cover the Coinbase Exchange Direct Feed signing helper used by
 * later authenticated subscribe and unsubscribe builders. Dependencies are kept
 * as real immutable {@link CoinbaseConfig} instances with fixed timestamps so
 * the suite can verify deterministic HMAC output, injectable clock behavior,
 * malformed secret handling, and redacted diagnostics without involving
 * transport or JSON payload construction.</p>
 */
class CoinbaseAuthenticatorTest {

    /**
     * Verifies deterministic Coinbase verify-request signing for a fixed timestamp.
     */
    @Test
    void signsVerifyRequestDeterministically() {
        CoinbaseAuthenticator authenticator = new CoinbaseAuthenticator(validConfig());

        CoinbaseAuthenticator.AuthFields fields = authenticator.authFields("1700000000");

        assertThat(fields.key()).isEqualTo("key-1");
        assertThat(fields.passphrase()).isEqualTo("passphrase-1");
        assertThat(fields.timestamp()).isEqualTo("1700000000");
        assertThat(fields.signature()).isEqualTo("lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg=");
    }

    /**
     * Verifies timestamp generation uses an injectable epoch clock.
     */
    @Test
    void authFieldsUseInjectableEpochClock() {
        CoinbaseAuthenticator authenticator = new CoinbaseAuthenticator(validConfig());

        CoinbaseAuthenticator.AuthFields fields = authenticator.authFields(() -> 1_700_000_000_999L);

        assertThat(fields.timestamp()).isEqualTo("1700000000");
        assertThat(fields.signature()).isEqualTo("lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg=");
    }

    /**
     * Verifies malformed Base64 secrets fail before payload construction and do not leak the configured value.
     */
    @Test
    void malformedSecretFailsWithoutSecretLeakage() {
        CoinbaseConfig config = new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "NOT_BASE64_SECRET_VALUE",
                "passphrase-1",
                List.of("level2", "heartbeat"));

        assertThatThrownBy(() -> new CoinbaseAuthenticator(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("apiSecret")
                .hasMessageNotContaining("NOT_BASE64_SECRET_VALUE");
    }

    /**
     * Verifies diagnostic output redacts authentication material.
     */
    @Test
    void toStringRedactsAuthValues() {
        CoinbaseAuthenticator authenticator = new CoinbaseAuthenticator(validConfig());
        CoinbaseAuthenticator.AuthFields fields = authenticator.authFields("1700000000");

        assertThat(authenticator.toString())
                .doesNotContain("key-1")
                .doesNotContain("passphrase-1");
        assertThat(fields.toString())
                .doesNotContain("key-1")
                .doesNotContain("passphrase-1")
                .doesNotContain(fields.signature());
    }

    private static CoinbaseConfig validConfig() {
        return new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("level2", "heartbeat"));
    }
}
