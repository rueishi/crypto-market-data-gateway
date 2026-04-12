package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoinbaseL2SubscriptionBuilder}.
 *
 * <p>The suite verifies deterministic Coinbase L2 subscribe and unsubscribe
 * payload construction using a real {@link CoinbaseAuthenticator} with fixed
 * credentials and an injectable clock. No network transport is involved; these
 * tests focus on the control-plane JSON scope owned by P2-002: product id,
 * required {@code level2}/{@code heartbeat} channels, stable auth fields, and
 * failure before send when required channels or instrument data are invalid.</p>
 */
class CoinbaseL2SubscriptionBuilderTest {
    private static final String EXPECTED_SIGNATURE = "lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg=";

    /**
     * Verifies the single subscribe payload contains product id, auth fields, and both required channels.
     */
    @Test
    void buildsDeterministicAuthenticatedSubscribePayload() {
        CoinbaseL2SubscriptionBuilder builder = newBuilder(validConfig());

        String payload = utf8(builder.buildSubscribe(instrument("BTC-USD")));

        assertThat(payload).isEqualTo(
                "{\"type\":\"subscribe\",\"product_ids\":[\"BTC-USD\"],\"channels\":[\"level2\",\"heartbeat\"],"
                        + "\"signature\":\"" + EXPECTED_SIGNATURE + "\",\"key\":\"key-1\","
                        + "\"passphrase\":\"passphrase-1\",\"timestamp\":\"1700000000\"}");
    }

    /**
     * Verifies unsubscribe mirrors the subscribe control-plane scope with only the type changed.
     */
    @Test
    void buildsDeterministicAuthenticatedUnsubscribePayload() {
        CoinbaseL2SubscriptionBuilder builder = newBuilder(validConfig());

        String payload = utf8(builder.buildUnsubscribe(instrument("BTC-USD")));

        assertThat(payload).isEqualTo(
                "{\"type\":\"unsubscribe\",\"product_ids\":[\"BTC-USD\"],\"channels\":[\"level2\",\"heartbeat\"],"
                        + "\"signature\":\"" + EXPECTED_SIGNATURE + "\",\"key\":\"key-1\","
                        + "\"passphrase\":\"passphrase-1\",\"timestamp\":\"1700000000\"}");
    }

    /**
     * Verifies optional extra channels remain in a stable single payload while required channels are present.
     */
    @Test
    void preservesConfiguredExtraChannelsAfterRequiredChannels() {
        CoinbaseConfig config = new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("level2", "heartbeat", "ticker"));
        CoinbaseL2SubscriptionBuilder builder = newBuilder(config);

        String payload = utf8(builder.buildSubscribe(instrument("ETH-USD")));

        assertThat(payload)
                .contains("\"product_ids\":[\"ETH-USD\"]")
                .contains("\"channels\":[\"level2\",\"heartbeat\",\"ticker\"]");
    }

    /**
     * Verifies missing heartbeat is rejected before the connector can send an invalid control-plane payload.
     */
    @Test
    void rejectsConfigThatOmitsRequiredHeartbeatChannel() {
        CoinbaseConfig config = new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("level2"));

        assertThatThrownBy(() -> newBuilder(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("level2 and heartbeat");
    }

    /**
     * Verifies malformed instrument input fails before JSON payload construction.
     */
    @Test
    void rejectsInstrumentWithoutExchangeSymbol() {
        CoinbaseL2SubscriptionBuilder builder = newBuilder(validConfig());

        assertThatThrownBy(() -> builder.buildSubscribe(instrument("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instrument.exchangeSymbol");
    }

    private static CoinbaseL2SubscriptionBuilder newBuilder(CoinbaseConfig config) {
        return new CoinbaseL2SubscriptionBuilder(config, new CoinbaseAuthenticator(config), () -> 1_700_000_000_999L);
    }

    private static CoinbaseConfig validConfig() {
        return new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("level2", "heartbeat"));
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
