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
    private static final String SAMPLE_UUID = "d50ec984-77a8-460a-b958-66f114b0de9b";
    private static final long SAMPLE_UUID_HIGH = 0xd50ec98477a8460aL;
    private static final long SAMPLE_UUID_LOW = 0xb95866f114b0de9bL;

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

    /**
     * Verifies the UUID parser writes the schema v2 high 64-bit half for a valid Coinbase order id.
     */
    @Test
    void parseUuidHighLow_validUuid_correctHighValue() {
        long[] result = new long[2];

        boolean parsed = ByteBufScanner.parseUuidHighLow(direct(SAMPLE_UUID), result);

        assertThat(parsed).isTrue();
        assertThat(result[0]).isEqualTo(SAMPLE_UUID_HIGH);
    }

    /**
     * Verifies the UUID parser writes the schema v2 low 64-bit half for a valid Coinbase order id.
     */
    @Test
    void parseUuidHighLow_validUuid_correctLowValue() {
        long[] result = new long[2];

        boolean parsed = ByteBufScanner.parseUuidHighLow(direct(SAMPLE_UUID), result);

        assertThat(parsed).isTrue();
        assertThat(result[1]).isEqualTo(SAMPLE_UUID_LOW);
    }

    /**
     * Verifies quoted UUID values consume both quotes so callers land on the following delimiter.
     */
    @Test
    void parseUuidHighLow_quotedUuid_skipsOpeningAndClosingQuote() {
        ByteBuf buf = direct("\"" + SAMPLE_UUID + "\",");
        long[] result = new long[2];

        boolean parsed = ByteBufScanner.parseUuidHighLow(buf, result);

        assertThat(parsed).isTrue();
        assertThat(result).containsExactly(SAMPLE_UUID_HIGH, SAMPLE_UUID_LOW);
        assertThat(buf.getByte(buf.readerIndex())).isEqualTo((byte) ',');
    }

    /**
     * Verifies the parsed high half matches the first eight bytes of the textual UUID.
     */
    @Test
    void parseUuidHighLow_highBytesMatchFirst8BytesOfUuid() {
        long[] result = new long[2];

        ByteBufScanner.parseUuidHighLow(direct(SAMPLE_UUID), result);

        assertThat(result[0]).isEqualTo(0xd5_0e_c9_84_77_a8_46_0aL);
    }

    /**
     * Verifies the parsed low half matches the final eight bytes of the textual UUID.
     */
    @Test
    void parseUuidHighLow_lowBytesMatchLast8BytesOfUuid() {
        long[] result = new long[2];

        ByteBufScanner.parseUuidHighLow(direct(SAMPLE_UUID), result);

        assertThat(result[1]).isEqualTo(0xb9_58_66_f1_14_b0_de_9bL);
    }

    /**
     * Verifies malformed non-hex content is rejected without exceptions and overwrites prior scratch values.
     */
    @Test
    void parseUuidHighLow_malformedUuid_writesSentinelAndReturnsFalse() {
        long[] result = {7L, 9L};

        boolean parsed = ByteBufScanner.parseUuidHighLow(direct("\"not-a-uuid-!!!!\""), result);

        assertThat(parsed).isFalse();
        assertThat(result).containsExactly(0L, 0L);
    }

    /**
     * Verifies truncated UUID content is rejected so L3 parsers cannot encode partial order ids.
     */
    @Test
    void parseUuidHighLow_truncatedUuid_writesSentinelAndReturnsFalse() {
        long[] result = {5L, 6L};

        boolean parsed = ByteBufScanner.parseUuidHighLow(direct("\"d50ec984-77a8-460a-b958\""), result);

        assertThat(parsed).isFalse();
        assertThat(result).containsExactly(0L, 0L);
    }

    /**
     * Verifies the reader index advances to the first byte after an unquoted UUID token.
     */
    @Test
    void parseUuidHighLow_readerIndexAdvancedPastUuid() {
        ByteBuf buf = direct(SAMPLE_UUID + "}");
        long[] result = new long[2];

        boolean parsed = ByteBufScanner.parseUuidHighLow(buf, result);

        assertThat(parsed).isTrue();
        assertThat(buf.getByte(buf.readerIndex())).isEqualTo((byte) '}');
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
