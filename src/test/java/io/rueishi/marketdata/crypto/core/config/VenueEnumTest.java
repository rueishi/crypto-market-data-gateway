package io.rueishi.marketdata.crypto.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link VenueEnum}.
 *
 * <p>These tests verify that each atomic exchange/depth identity carries the
 * byte value, {@link BookDepth}, and {@link TemplateId} assigned by the spec.
 * They also cover parsing failures because config validation depends on this
 * enum as the first venue boundary.</p>
 */
class VenueEnumTest {

    /**
     * Verifies the pre-allocated venue byte values and their book-depth/template consistency.
     */
    @Test
    void venueConstantsCarryAssignedByteDepthAndTemplateValues() {
        assertVenue(VenueEnum.COINBASE_L2, 1, BookDepth.L2, TemplateId.BOOK_LEVEL, "coinbase");
        assertVenue(VenueEnum.COINBASE_L3, 2, BookDepth.L3, TemplateId.ORDER_EVENT, "coinbase");
        assertVenue(VenueEnum.BINANCE_L2, 3, BookDepth.L2, TemplateId.BOOK_LEVEL, "binance");
        assertVenue(VenueEnum.BINANCE_L3, 4, BookDepth.L3, TemplateId.ORDER_EVENT, "binance");
        assertVenue(VenueEnum.KRAKEN_L2, 5, BookDepth.L2, TemplateId.BOOK_LEVEL, "kraken");
        assertVenue(VenueEnum.OKX_L2, 6, BookDepth.L2, TemplateId.BOOK_LEVEL, "okx");
        assertVenue(VenueEnum.OKX_L3, 7, BookDepth.L3, TemplateId.ORDER_EVENT, "okx");
        assertVenue(VenueEnum.BYBIT_L2, 8, BookDepth.L2, TemplateId.BOOK_LEVEL, "bybit");
    }

    /**
     * Verifies that venue parsing accepts exact enum names and rejects missing or unknown values.
     */
    @Test
    void parseRequiresRecognizedVenueName() {
        assertThat(VenueEnum.parse("COINBASE_L2")).isEqualTo(VenueEnum.COINBASE_L2);

        assertThatThrownBy(() -> VenueEnum.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("venue is required");
        assertThatThrownBy(() -> VenueEnum.parse("coinbase_l2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown venue")
                .hasMessageContaining("COINBASE_L2");
    }

    private static void assertVenue(
            VenueEnum venue,
            int byteValue,
            BookDepth bookDepth,
            TemplateId templateId,
            String venueConfigKey) {
        assertThat(venue.byteValue()).isEqualTo((byte) byteValue);
        assertThat(venue.bookDepth()).isEqualTo(bookDepth);
        assertThat(venue.templateId()).isEqualTo(templateId);
        assertThat(venue.venueConfigKey()).isEqualTo(venueConfigKey);
    }
}
