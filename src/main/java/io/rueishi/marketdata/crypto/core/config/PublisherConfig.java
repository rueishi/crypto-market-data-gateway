package io.rueishi.marketdata.crypto.core.config;

/**
 * Publisher selection block from startup configuration.
 *
 * <p>{@link #type} is intentionally a {@link String}, not
 * {@link PublisherType}. SnakeYAML can then bind unknown values without
 * throwing, allowing {@link ConfigValidator} and later bootstrap publisher
 * wiring to produce controlled {@link IllegalArgumentException} messages that
 * name the valid values.</p>
 */
public final class PublisherConfig {
    /** Publisher backend text parsed to {@link PublisherType} during validation/wiring. */
    public String type;
    /** Logging publisher options used only when {@link #type} is {@code LOGGING}. */
    public LoggingPublisherConfig logging;
}
