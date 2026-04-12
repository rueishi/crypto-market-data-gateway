package io.rueishi.marketdata.crypto.core.config;

/**
 * Identifies the encoded message template and repeating-group layout.
 *
 * <p>{@code TemplateId} is carried by {@link VenueEnum} and selected once when
 * connector factory code constructs an encoder. {@link #BOOK_LEVEL} identifies
 * aggregated book-level entries, while {@link #ORDER_ENTRY} identifies L3
 * order-entry messages. The byte value is part of the wire header and is
 * immutable once assigned.</p>
 */
public enum TemplateId {
    /** Repeating group contains aggregated price-level entries. */
    BOOK_LEVEL((byte) 1),
    /** Repeating group contains individual order entries. */
    ORDER_ENTRY((byte) 2);

    private final byte byteValue;

    TemplateId(byte byteValue) {
        this.byteValue = byteValue;
    }

    /**
     * Returns the fixed byte value written into encoded message headers.
     *
     * @return the immutable wire value for this template id
     */
    public byte byteValue() {
        return byteValue;
    }
}
