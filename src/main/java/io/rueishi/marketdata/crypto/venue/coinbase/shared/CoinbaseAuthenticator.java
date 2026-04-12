package io.rueishi.marketdata.crypto.venue.coinbase.shared;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.agrona.concurrent.EpochClock;

/**
 * Coinbase Exchange Direct Feed authentication helper.
 *
 * <p>{@code CoinbaseAuthenticator} owns the Coinbase control-plane signing
 * algorithm shared by subscribe and unsubscribe builders. A connector factory
 * creates it from validated {@link CoinbaseConfig} during startup and later L2
 * subscription builders call it when constructing authenticated JSON payloads
 * for {@code level2} and {@code heartbeat}. It does not send network messages
 * or build JSON; it only produces timestamp, signature, key, and passphrase
 * values.</p>
 *
 * <p>The Coinbase API secret is decoded once during construction, and each
 * signing call uses the Exchange API prehash string
 * {@code timestamp + "GET" + "/users/self/verify"}. Secret values are never
 * included in exception messages or diagnostic {@code toString()} output.</p>
 */
public final class CoinbaseAuthenticator {
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String VERIFY_METHOD = "GET";
    private static final String VERIFY_PATH = "/users/self/verify";

    private final String apiKey;
    private final String apiPassphrase;
    private final byte[] apiSecretBytes;

    /**
     * Creates an authenticator from typed Coinbase configuration.
     *
     * <p>This constructor is called during connector factory setup, before any
     * control-plane payload is built. It validates that the configured
     * {@code apiSecret} is valid Base64 because Coinbase Exchange secrets are
     * supplied in Base64 form and must be decoded before HMAC signing.</p>
     *
     * @param config typed Coinbase venue configuration
     * @throws NullPointerException if {@code config} is null
     * @throws IllegalArgumentException if the configured API secret is not valid Base64
     */
    public CoinbaseAuthenticator(CoinbaseConfig config) {
        Objects.requireNonNull(config, "config");
        this.apiKey = config.apiKey();
        this.apiPassphrase = config.apiPassphrase();
        try {
            this.apiSecretBytes = Base64.getDecoder().decode(config.apiSecret());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("apiSecret must be valid Base64", ex);
        }
    }

    /**
     * Builds Coinbase authentication fields using an injectable clock.
     *
     * <p>Subscription builders call this on the control path immediately before
     * serializing a subscribe or unsubscribe payload. The injected clock keeps
     * time-dependent behavior deterministic in tests and avoids direct calls to
     * system time from venue code.</p>
     *
     * @param epochClock clock returning epoch milliseconds
     * @return key, passphrase, timestamp, and signature values for a Coinbase control-plane message
     * @throws NullPointerException if {@code epochClock} is null
     * @throws IllegalStateException if the JVM does not provide HMAC-SHA256
     */
    public AuthFields authFields(EpochClock epochClock) {
        Objects.requireNonNull(epochClock, "epochClock");
        return authFields(timestampSeconds(epochClock.time()));
    }

    /**
     * Builds Coinbase authentication fields for a fixed timestamp.
     *
     * <p>Tests and deterministic payload builders use this overload when the
     * timestamp has already been captured. The timestamp must be non-blank and
     * is signed exactly as supplied.</p>
     *
     * @param timestamp Unix epoch seconds string used in the Coinbase prehash
     * @return key, passphrase, timestamp, and signature values for a Coinbase control-plane message
     * @throws IllegalArgumentException if {@code timestamp} is blank
     * @throws IllegalStateException if the JVM does not provide HMAC-SHA256
     */
    public AuthFields authFields(String timestamp) {
        String validatedTimestamp = requireText(timestamp, "timestamp");
        return new AuthFields(apiKey, apiPassphrase, validatedTimestamp, signVerifyRequest(validatedTimestamp));
    }

    /**
     * Converts epoch milliseconds to the Coinbase timestamp string.
     *
     * <p>Coinbase expects the control-plane timestamp as Unix epoch seconds.
     * This helper truncates milliseconds because the Phase 2 spec calls for the
     * current epoch second as a string.</p>
     *
     * @param epochMillis epoch milliseconds from an injectable {@link EpochClock}
     * @return epoch seconds string
     */
    public String timestampSeconds(long epochMillis) {
        return Long.toString(epochMillis / 1_000L);
    }

    /**
     * Signs the Coinbase Exchange verify prehash for a timestamp.
     *
     * <p>The prehash is {@code timestamp + "GET" + "/users/self/verify"}.
     * The method Base64-encodes the HMAC-SHA256 result for insertion into
     * authenticated subscribe and unsubscribe payloads.</p>
     *
     * @param timestamp Unix epoch seconds string used in the Coinbase prehash
     * @return Base64-encoded HMAC-SHA256 signature
     * @throws IllegalArgumentException if {@code timestamp} is blank
     * @throws IllegalStateException if the JVM does not provide HMAC-SHA256
     */
    public String signVerifyRequest(String timestamp) {
        String validatedTimestamp = requireText(timestamp, "timestamp");
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(apiSecretBytes, HMAC_ALGORITHM));
            byte[] prehash = (validatedTimestamp + VERIFY_METHOD + VERIFY_PATH).getBytes(StandardCharsets.US_ASCII);
            return Base64.getEncoder().encodeToString(mac.doFinal(prehash));
        } catch (java.security.GeneralSecurityException ex) {
            throw new IllegalStateException("Unable to sign Coinbase authentication prehash", ex);
        }
    }

    /**
     * Returns a redacted diagnostic representation.
     *
     * @return authenticator summary without key, passphrase, or secret material
     */
    @Override
    public String toString() {
        return "CoinbaseAuthenticator[apiKey=<redacted>, apiPassphrase=<redacted>]";
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    /**
     * Authentication fields inserted into Coinbase control-plane payloads.
     *
     * <p>The record is returned by {@link CoinbaseAuthenticator} and consumed by
     * later subscription builders. Its accessors expose the raw values needed
     * for JSON serialization, while {@link #toString()} keeps diagnostic output
     * redacted.</p>
     *
     * @param key Coinbase API key
     * @param passphrase Coinbase API passphrase
     * @param timestamp Unix epoch seconds string
     * @param signature Base64-encoded HMAC-SHA256 signature
     */
    public record AuthFields(String key, String passphrase, String timestamp, String signature) {

        /**
         * Creates an immutable authentication field bundle.
         *
         * @throws IllegalArgumentException if any field is blank
         */
        public AuthFields {
            key = requireText(key, "key");
            passphrase = requireText(passphrase, "passphrase");
            timestamp = requireText(timestamp, "timestamp");
            signature = requireText(signature, "signature");
        }

        /**
         * Returns redacted diagnostic output for authentication values.
         *
         * @return summary without key, passphrase, or signature material
         */
        @Override
        public String toString() {
            return "AuthFields[key=<redacted>, passphrase=<redacted>, timestamp="
                    + timestamp + ", signature=<redacted>]";
        }
    }
}
