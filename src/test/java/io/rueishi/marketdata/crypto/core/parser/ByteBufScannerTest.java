package io.rueishi.marketdata.crypto.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ByteBufScanner}.
 *
 * <p>The tests exercise the shared parser utility that future venue parsers
 * will call on direct Netty buffers. They use caller-owned primitive scratch
 * arrays and byte arrays instead of mocks because the component has no external
 * collaborators. The scenarios cover required decimal vectors, malformed and
 * overflow sentinels, RFC 3339 timestamp conversion, and small JSON scanning
 * helpers used by venue-specific parser code.</p>
 */
class ByteBufScannerTest {

    /**
     * Verifies all decimal vectors required by the Phase 1 specification.
     */
    @Test
    void parsesRequiredDecimalVectorsFromDirectBuffers() {
        assertDecimal("21921.73", 2192173L, 2);
        assertDecimal("0.06317902", 6317902L, 8);
        assertDecimal("21922.10", 2192210L, 2);
        assertDecimal("100", 100L, 0);
        assertDecimal("0", 0L, 0);
        assertDecimal("-0.5", -5L, 1);
    }

    /**
     * Protects against floating-point rounding regressions by using a value that must remain exact as integers.
     */
    @Test
    void parsesLargeDecimalWithoutFloatingPointRounding() {
        assertDecimal("123456789.12345678", 12345678912345678L, 8);
        assertDecimal("922337203685477580.7", Long.MAX_VALUE, 1);
    }

    /**
     * Verifies the documented malformed and overflow sentinel contract for decimal parsing.
     */
    @Test
    void malformedDecimalWritesSentinelPair() {
        assertDecimalSentinel("9223372036854775808");
        assertDecimalSentinel("1e-8");
        assertDecimalSentinel("1.1234567890123456789");
        assertDecimalSentinel("\"\"");
    }

    /**
     * Verifies that quoted decimal parsing leaves the reader positioned at the closing quote.
     */
    @Test
    void quotedDecimalLeavesReaderIndexAtClosingQuote() {
        ByteBuf buf = direct("\"21922.10\",");
        long[] result = new long[2];

        ByteBufScanner.parseDecimal(buf, result);

        assertThat(result).containsExactly(2192210L, 2L);
        assertThat(buf.getByte(buf.readerIndex())).isEqualTo((byte) '"');
    }

    /**
     * Verifies UTC RFC 3339 timestamp conversion with and without fractional seconds.
     */
    @Test
    void parsesRfc3339TimestampsToEpochNanos() {
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"1970-01-01T00:00:01Z\"")))
                .isEqualTo(1_000_000_000L);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"2023-02-09T20:33:09.999999Z\"")))
                .isEqualTo(1_675_974_789_999_999_000L);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("2024-02-29T00:00:00.1Z")))
                .isEqualTo(1_709_164_800_100_000_000L);
    }

    /**
     * Verifies absent, empty, epoch-zero, and malformed timestamp values use the same sentinel.
     */
    @Test
    void timestampSentinelCoversAbsentEpochZeroAndMalformedValues() {
        assertThat(ByteBufScanner.parseTimestampNanos(direct("null"))).isEqualTo(ByteBufScanner.TIMESTAMP_SENTINEL);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"\""))).isEqualTo(ByteBufScanner.TIMESTAMP_SENTINEL);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"1970-01-01T00:00:00Z\"")))
                .isEqualTo(ByteBufScanner.TIMESTAMP_SENTINEL);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"2023-02-30T20:33:09Z\"")))
                .isEqualTo(ByteBufScanner.TIMESTAMP_SENTINEL);
        assertThat(ByteBufScanner.parseTimestampNanos(direct("\"2023-02-09T20:33:09.1234567890Z\"")))
                .isEqualTo(ByteBufScanner.TIMESTAMP_SENTINEL);
    }

    /**
     * Verifies key scanning, string copying, integer reading, array movement, and value skipping.
     */
    @Test
    void scansCommonJsonTokensWithoutStringAllocation() {
        ByteBuf buf = direct("{\"type\":\"l2update\",\"product_id\":\"BTC-USD\",\"levels\":[1,{\"skip\":true},3]}");
        byte[] product = new byte[16];

        assertThat(ByteBufScanner.scanToKey(buf, "product_id".getBytes(StandardCharsets.US_ASCII))).isTrue();
        assertThat(ByteBufScanner.readStringInto(buf, product)).isEqualTo(7);
        assertThat(product).startsWith((byte) 'B', (byte) 'T', (byte) 'C', (byte) '-', (byte) 'U', (byte) 'S', (byte) 'D');

        assertThat(ByteBufScanner.scanToKey(buf, "\"levels\":".getBytes(StandardCharsets.US_ASCII))).isTrue();
        assertThat(buf.getByte(buf.readerIndex())).isEqualTo((byte) '[');
        buf.readerIndex(buf.readerIndex() + 1);
        assertThat(ByteBufScanner.nextArrayElement(buf)).isTrue();
        assertThat(ByteBufScanner.readLong(buf)).isEqualTo(1L);
        assertThat(ByteBufScanner.nextArrayElement(buf)).isTrue();
        ByteBufScanner.skipValue(buf);
        assertThat(ByteBufScanner.nextArrayElement(buf)).isTrue();
        assertThat(ByteBufScanner.readLong(buf)).isEqualTo(3L);
        assertThat(ByteBufScanner.nextArrayElement(buf)).isFalse();
    }

    private static void assertDecimal(String text, long expectedMantissa, long expectedScale) {
        long[] result = new long[2];

        ByteBufScanner.parseDecimal(direct(text), result);

        assertThat(result).containsExactly(expectedMantissa, expectedScale);
    }

    private static void assertDecimalSentinel(String text) {
        long[] result = new long[2];

        ByteBufScanner.parseDecimal(direct(text), result);

        assertThat(result).containsExactly(ByteBufScanner.DECIMAL_SENTINEL_MANTISSA,
                ByteBufScanner.DECIMAL_SENTINEL_SCALE);
    }

    private static ByteBuf direct(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        ByteBuf buf = Unpooled.directBuffer(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }
}
