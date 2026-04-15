package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoinbaseL3SubscriptionBuilder}.
 *
 * <p>The suite verifies deterministic Coinbase full-feed subscribe and
 * unsubscribe payload construction using a real
 * {@link CoinbaseAuthenticator}, fixed credentials, and an injectable clock.
 * No network transport is involved; the tests focus on the control-plane JSON
 * scope owned by P4-B3: product id, required {@code full}/{@code heartbeat}
 * channels, stable authentication fields, and correct subscribe versus
 * unsubscribe payload types.</p>
 */
class CoinbaseL3SubscriptionBuilderTest {

    /**
     * Verifies the L3 subscribe payload uses the Coinbase subscribe control-plane type.
     */
    @Test
    void buildSubscribe_typeIsSubscribe() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .contains("\"type\":\"subscribe\"");
    }

    /**
     * Verifies the L3 full channel is always present because the parser depends on it for order events.
     */
    @Test
    void buildSubscribe_containsFullChannel() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .contains("\"full\"");
    }

    /**
     * Verifies the heartbeat channel is present so connector liveness checks have a control-plane source.
     */
    @Test
    void buildSubscribe_containsHeartbeatChannel() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .contains("\"heartbeat\"");
    }

    /**
     * Verifies the configured instrument exchange symbol becomes the outbound Coinbase product id.
     */
    @Test
    void buildSubscribe_productIdMatchesInstrument() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .contains("\"product_ids\":[\"BTC-USD\"]");
    }

    /**
     * Verifies a non-empty Coinbase signature is generated from the shared authenticator for each payload.
     */
    @Test
    void buildSubscribe_signatureIsNonEmpty() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .containsPattern("\"signature\":\"[^\"]+\"");
    }

    /**
     * Verifies all required Coinbase authentication fields are present in the subscribe payload.
     */
    @Test
    void buildSubscribe_allAuthFieldsPresent() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildSubscribe(instrument("BTC-USD"))))
                .contains("\"key\":\"key-1\"")
                .contains("\"passphrase\":\"passphrase-1\"")
                .contains("\"timestamp\":\"1700000000\"");
    }

    /**
     * Verifies unsubscribe mirrors the subscribe scope while switching only the control-plane type.
     */
    @Test
    void buildUnsubscribe_typeIsUnsubscribe() {
        CoinbaseL3SubscriptionBuilder builder = newBuilder();

        assertThat(utf8(builder.buildUnsubscribe(instrument("BTC-USD"))))
                .contains("\"type\":\"unsubscribe\"");
    }

    private static CoinbaseL3SubscriptionBuilder newBuilder() {
        CoinbaseConfig config = new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("full", "heartbeat"));
        return new CoinbaseL3SubscriptionBuilder(config, new CoinbaseAuthenticator(config), () -> 1_700_000_000_999L);
    }

    private static InstrumentConfig instrument(String exchangeSymbol) {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = exchangeSymbol;
        instrument.instrumentId = 1001;
        return instrument;
    }

    private static String utf8(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
