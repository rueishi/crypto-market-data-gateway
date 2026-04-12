package io.rueishi.marketdata.crypto.core.config;

/**
 * Encoder capacity configuration loaded during startup.
 *
 * <p>Connector factory code uses this object to size the reusable encoder
 * buffer for the configured venue and book depth. Validation happens before any
 * connector is created so malformed capacities fail during startup rather than
 * during message processing.</p>
 */
public final class EncodingConfig {
    /** Maximum number of level or order entries allowed in one encoded message. */
    public int maxLevelsPerMessage;
    /** Extra bytes reserved beyond the computed maximum message size. */
    public int bufferHeadroomBytes;
}
