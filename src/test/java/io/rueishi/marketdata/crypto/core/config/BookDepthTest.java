package io.rueishi.marketdata.crypto.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BookDepth}.
 *
 * <p>These tests verify the immutable byte values used by encoder construction
 * and downstream message decoding. They use no mocks because the enum is a
 * self-contained wire-contract type.</p>
 */
class BookDepthTest {

    /**
     * Verifies that depth levels keep the byte values assigned by the Phase 1 wire contract.
     */
    @Test
    void depthLevelsCarryAssignedWireValues() {
        assertThat(BookDepth.L1.byteValue()).isEqualTo((byte) 1);
        assertThat(BookDepth.L2.byteValue()).isEqualTo((byte) 2);
        assertThat(BookDepth.L3.byteValue()).isEqualTo((byte) 3);
    }
}
