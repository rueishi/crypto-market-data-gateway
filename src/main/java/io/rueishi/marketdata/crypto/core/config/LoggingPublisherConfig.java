package io.rueishi.marketdata.crypto.core.config;

/**
 * Configuration for the future integration-validation logging publisher.
 *
 * <p>The logging publisher itself is still a placeholder at this phase, but
 * configuration validation needs this object so a {@code LOGGING} publisher
 * selection can fail fast when mandatory file output settings are missing.</p>
 */
public final class LoggingPublisherConfig {
    /** Path to the rolling structured message log. */
    public String outputPath;
    /** Maximum log size in MiB before rotation. */
    public int rollSizeMb;
}
