package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import org.junit.jupiter.api.Test;

/**
 * Bounds and state-machine tests for {@link SbeEncoder}.
 *
 * <p>These tests protect the fixed-capacity direct-buffer contract by verifying
 * oversize messages are rejected and that malformed call sequences fail before
 * the reusable buffer can be corrupted.</p>
 */
class SbeEncoderOverflowTest {

    /**
     * Verifies that writing more than the configured entry capacity fails without growing the buffer.
     */
    @Test
    void rejectsEntriesBeyondConfiguredMaximumWithoutIncreasingCapacity() {
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);
        int capacity = encoder.capacity();

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 1L);
        encoder.writeLevel((byte) 1, (byte) 1, 10L, (byte) 1, 20L, (byte) 2);

        assertThatThrownBy(() -> encoder.writeLevel((byte) 2, (byte) 1, 11L, (byte) 1, 21L, (byte) 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxEntryCount");
        assertThat(encoder.capacity()).isEqualTo(capacity);
    }

    /**
     * Verifies invalid call ordering fails clearly for parser/connector misuse.
     */
    @Test
    void rejectsMalformedMessageLifecycleCalls() {
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        assertThatThrownBy(() -> encoder.writeLevel((byte) 1, (byte) 1, 1L, (byte) 0, 1L, (byte) 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No message");

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 1L);

        assertThatThrownBy(() -> encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 2, 2, 2, -1L, 2L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already in progress");
    }
}
