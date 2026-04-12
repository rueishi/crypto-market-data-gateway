package io.rueishi.marketdata.crypto.core.config;

/**
 * Publisher backend selector from startup configuration.
 *
 * <p>{@link PublisherConfig#type} remains a {@link String} so YAML binding does
 * not throw before application validation can produce controlled startup
 * messages. This enum records the accepted values that bootstrap and later
 * publisher wiring use after validation.</p>
 */
public enum PublisherType {
    /** Integration validation adapter that writes structured summaries to a rolling log. */
    LOGGING,
    /** Test adapter that captures messages in a bounded in-memory ring. */
    IN_MEMORY,
    /** Future Agrona shared-memory IPC publisher. */
    SHARED_MEMORY,
    /** Future Aeron publisher. */
    AERON,
    /** Future Chronicle Queue publisher. */
    CHRONICLE
}
