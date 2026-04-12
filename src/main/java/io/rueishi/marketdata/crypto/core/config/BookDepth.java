package io.rueishi.marketdata.crypto.core.config;

/**
 * Market-data depth level produced by a venue strategy and encoded in every gateway message.
 *
 * <p>{@code BookDepth} is carried by {@link VenueEnum} so connector factories
 * can configure encoders with the correct fixed byte value at startup. The byte
 * value is part of the downstream wire contract and must not change after it is
 * assigned.</p>
 */
public enum BookDepth {
    /** Top-of-book data containing only the best bid and best ask. */
    L1((byte) 1),
    /** Aggregated price levels with total quantity per price level. */
    L2((byte) 2),
    /** Individual resting orders, one entry per order. */
    L3((byte) 3);

    private final byte byteValue;

    BookDepth(byte byteValue) {
        this.byteValue = byteValue;
    }

    /**
     * Returns the fixed byte value written into encoded message bodies.
     *
     * @return the immutable wire value for this depth level
     */
    public byte byteValue() {
        return byteValue;
    }
}
