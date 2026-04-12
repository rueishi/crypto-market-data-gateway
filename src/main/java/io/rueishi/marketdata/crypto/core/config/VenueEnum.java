package io.rueishi.marketdata.crypto.core.config;

import java.util.Locale;

/**
 * Atomic exchange-and-depth identity for a running gateway instance.
 *
 * <p>A gateway process is configured with exactly one {@code VenueEnum}. The
 * enum combines the exchange and depth level, carries the stable venue byte
 * used by the encoder, and exposes the matching {@link BookDepth} and
 * {@link TemplateId}. Bootstrap and connector factory code use this type to
 * route venue discovery and encoder setup without scattering byte constants
 * across the system.</p>
 */
public enum VenueEnum {
    /** Coinbase level 2 aggregated book feed. */
    COINBASE_L2((byte) 1, BookDepth.L2, TemplateId.BOOK_LEVEL),
    /** Coinbase level 3 full order feed. */
    COINBASE_L3((byte) 2, BookDepth.L3, TemplateId.ORDER_ENTRY),
    /** Binance level 2 depth feed; value reserved before venue implementation arrives. */
    BINANCE_L2((byte) 3, BookDepth.L2, TemplateId.BOOK_LEVEL),
    /** Binance level 3 order feed; value reserved before venue implementation arrives. */
    BINANCE_L3((byte) 4, BookDepth.L3, TemplateId.ORDER_ENTRY),
    /** Kraken level 2 depth feed; value reserved before venue implementation arrives. */
    KRAKEN_L2((byte) 5, BookDepth.L2, TemplateId.BOOK_LEVEL),
    /** OKX level 2 depth feed; value reserved before venue implementation arrives. */
    OKX_L2((byte) 6, BookDepth.L2, TemplateId.BOOK_LEVEL),
    /** OKX level 3 order feed; value reserved before venue implementation arrives. */
    OKX_L3((byte) 7, BookDepth.L3, TemplateId.ORDER_ENTRY),
    /** Bybit level 2 depth feed; value reserved before venue implementation arrives. */
    BYBIT_L2((byte) 8, BookDepth.L2, TemplateId.BOOK_LEVEL);

    private final byte byteValue;
    private final BookDepth bookDepth;
    private final TemplateId templateId;

    VenueEnum(byte byteValue, BookDepth bookDepth, TemplateId templateId) {
        this.byteValue = byteValue;
        this.bookDepth = bookDepth;
        this.templateId = templateId;
    }

    /**
     * Parses a YAML venue value into a known venue identity.
     *
     * @param value venue text from configuration
     * @return the matching venue enum constant
     * @throws IllegalArgumentException if the value is null, blank, or not recognized
     */
    public static VenueEnum parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("venue is required. Valid values: " + validValues());
        }
        try {
            return VenueEnum.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    "Unknown venue '" + value + "'. Valid values: " + validValues(), ex);
        }
    }

    /**
     * Returns the fixed venue byte used in encoded messages.
     *
     * @return immutable venue wire value
     */
    public byte byteValue() {
        return byteValue;
    }

    /**
     * Returns the book depth associated with this exchange/depth identity.
     *
     * @return the configured book depth
     */
    public BookDepth bookDepth() {
        return bookDepth;
    }

    /**
     * Returns the encoded message template associated with this venue identity.
     *
     * @return the configured template id
     */
    public TemplateId templateId() {
        return templateId;
    }

    /**
     * Returns the lowercase top-level venue block key expected in YAML.
     *
     * <p>For example, {@code COINBASE_L2} maps to {@code coinbase}. This is used
     * by {@link ConfigLoader} to extract the raw venue-specific configuration
     * block after the common fields are bound.</p>
     *
     * @return lowercase venue-specific YAML block key
     */
    public String venueConfigKey() {
        int separator = name().indexOf('_');
        String prefix = separator < 0 ? name() : name().substring(0, separator);
        return prefix.toLowerCase(Locale.ROOT);
    }

    /**
     * Returns a comma-separated list of valid configuration names.
     *
     * @return valid venue names for validation messages
     */
    public static String validValues() {
        VenueEnum[] vals = values();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < vals.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(vals[i].name());
        }
        return sb.toString();
    }
}
