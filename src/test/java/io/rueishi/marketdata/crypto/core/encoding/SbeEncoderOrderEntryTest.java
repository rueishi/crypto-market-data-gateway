package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import org.junit.jupiter.api.Test;

/**
 * Byte-layout tests for {@link SbeEncoder} using the ORDER_ENTRY template.
 *
 * <p>These tests cover the L3-style repeating group and contract failures for
 * calling the wrong writer method for a configured template.</p>
 */
class SbeEncoderOrderEntryTest {

    /**
     * Verifies one ORDER_ENTRY message uses the 29-byte order-entry layout and L3 venue metadata.
     */
    @Test
    void encodesOrderEntryMessageLittleEndian() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(
                2002,
                VenueEnum.COINBASE_L3.byteValue(),
                BookDepth.L3.byteValue(),
                TemplateId.ORDER_ENTRY.byteValue(),
                10,
                0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 20, 21, 22, 55L, 66L);
        encoder.writeOrder(9_999L, (byte) 2, (byte) 1, (byte) 1, 1_001_000L, (byte) 3, 50_000L, (byte) 4);

        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isTrue();

        SbeDecoder decoded = new SbeDecoder(publisher.lastMessage());
        assertThat(decoded.templateId()).isEqualTo(TemplateId.ORDER_ENTRY.byteValue());
        assertThat(decoded.bookDepth()).isEqualTo(BookDepth.L3.byteValue());
        assertThat(decoded.venue()).isEqualTo(VenueEnum.COINBASE_L3.byteValue());
        assertThat(decoded.entryCount()).isEqualTo(1);
        assertThat(decoded.orderId(0)).isEqualTo(9_999L);
        assertThat(decoded.orderSide(0)).isEqualTo(2);
        assertThat(decoded.orderAction(0)).isEqualTo(1);
        assertThat(decoded.orderType(0)).isEqualTo(1);
        assertThat(decoded.orderPriceScale(0)).isEqualTo(3);
        assertThat(decoded.orderQtyScale(0)).isEqualTo(4);
        assertThat(decoded.orderPriceMantissa(0)).isEqualTo(1_001_000L);
        assertThat(decoded.orderQtyMantissa(0)).isEqualTo(50_000L);
        assertThat(publisher.lastLength()).isEqualTo(
                EncodingConstants.MESSAGE_PREFIX_LENGTH + EncodingConstants.ORDER_ENTRY_LENGTH);
    }

    /**
     * Verifies that template-specific writers fail fast instead of corrupting message bytes.
     */
    @Test
    void rejectsWriterMethodThatDoesNotMatchConfiguredTemplate() {
        SbeEncoder levelEncoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);
        levelEncoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 1L);

        assertThatThrownBy(() -> levelEncoder.writeOrder(1L, (byte) 1, (byte) 1, (byte) 1, 1L, (byte) 0, 1L, (byte) 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ORDER_ENTRY");

        SbeEncoder orderEncoder = new SbeEncoder(1001, (byte) 2, (byte) 3, TemplateId.ORDER_ENTRY.byteValue(), 1, 0);
        orderEncoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 1L);

        assertThatThrownBy(() -> orderEncoder.writeLevel((byte) 1, (byte) 1, 1L, (byte) 0, 1L, (byte) 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("BOOK_LEVEL");
    }
}
