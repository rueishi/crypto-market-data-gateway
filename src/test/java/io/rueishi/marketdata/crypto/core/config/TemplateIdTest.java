package io.rueishi.marketdata.crypto.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TemplateId}.
 *
 * <p>These tests pin the immutable template id bytes that later encoder tests
 * and downstream decoders use to select the correct repeating-group layout.</p>
 */
class TemplateIdTest {

    /**
     * Verifies that template ids keep the byte values assigned by the Phase 1 wire contract.
     */
    @Test
    void templateIdsCarryAssignedWireValues() {
        assertThat(TemplateId.BOOK_LEVEL.byteValue()).isEqualTo((byte) 1);
        assertThat(TemplateId.ORDER_EVENT.byteValue()).isEqualTo((byte) 2);
        assertThat(TemplateId.TRADE_EVENT.byteValue()).isEqualTo((byte) 3);
    }
}
