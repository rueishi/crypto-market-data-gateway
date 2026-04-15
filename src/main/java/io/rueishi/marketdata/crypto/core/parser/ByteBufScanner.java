package io.rueishi.marketdata.crypto.core.parser;

import io.netty.buffer.ByteBuf;
import java.util.Objects;

/**
 * Allocation-free utility methods for scanning JSON payloads directly from Netty {@link ByteBuf} instances.
 *
 * <p>Venue {@link FeedParser} implementations use this class on the event-loop
 * frame path to find JSON fields, copy stable byte slices into caller-owned
 * scratch buffers, parse fixed-point decimals, and convert exchange timestamps
 * without materializing {@link String} or {@code java.time} objects. The class
 * is stateless and thread-safe; all mutable parse output is written into
 * caller-supplied primitive arrays or the buffer's reader index.</p>
 *
 * <p>Decimal failures use the documented sentinel {@code mantissa =
 * Long.MAX_VALUE, scale = 0}. That sentinel intentionally keeps parsing
 * allocation-free, but it means the exact fixed-point value {@code
 * 9223372036854775807} is indistinguishable from overflow and should be
 * rejected by callers that check the sentinel. Timestamp failures, absent
 * values, empty values, and epoch-zero values return {@link #TIMESTAMP_SENTINEL}.
 * Callers are expected to translate those sentinel values into
 * malformed-message rejection or absent-timestamp handling in venue-specific
 * parser code.</p>
 */
public final class ByteBufScanner {
    /**
     * Decimal parse failure sentinel written to {@code result[0]}.
     */
    public static final long DECIMAL_SENTINEL_MANTISSA = Long.MAX_VALUE;

    /**
     * Decimal parse failure sentinel written to {@code result[1]}.
     */
    public static final long DECIMAL_SENTINEL_SCALE = 0L;

    /**
     * Timestamp parse failure, absent-value, and epoch-zero sentinel.
     */
    public static final long TIMESTAMP_SENTINEL = -1L;

    private static final byte QUOTE = '"';
    private static final byte COLON = ':';
    private static final byte COMMA = ',';
    private static final byte ARRAY_END = ']';
    private static final byte OBJECT_END = '}';
    private static final byte DECIMAL_POINT = '.';
    private static final byte MINUS = '-';
    private static final int MAX_DECIMAL_SCALE = 18;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long SECONDS_PER_DAY = 86_400L;

    private ByteBufScanner() {
    }

    /**
     * Advances the reader index to just after the next matching JSON key and colon.
     *
     * <p>If {@code asciiKey} begins with a quote, it is treated as an exact byte
     * pattern to find, for callers that precompute a token such as
     * {@code "\"price\":"}. Otherwise the method searches for a JSON object
     * key named by {@code asciiKey}, accepts optional whitespace before the
     * colon, and leaves the reader index immediately after the colon. It returns
     * {@code false} without changing the reader index when the key is absent.</p>
     *
     * @param buf buffer to scan from its current reader index to writer index
     * @param asciiKey ASCII key bytes, either bare key text or an exact quoted token
     * @return true if the key was found and the reader index was advanced
     * @throws NullPointerException if {@code buf} or {@code asciiKey} is null
     */
    public static boolean scanToKey(ByteBuf buf, byte[] asciiKey) {
        Objects.requireNonNull(buf, "buf");
        Objects.requireNonNull(asciiKey, "asciiKey");
        if (asciiKey.length == 0) {
            return false;
        }

        int originalReaderIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        if (asciiKey[0] == QUOTE) {
            int exactEnd = scanExact(buf, originalReaderIndex, writerIndex, asciiKey);
            if (exactEnd >= 0) {
                buf.readerIndex(exactEnd);
                return true;
            }
            return false;
        }

        for (int index = originalReaderIndex; index < writerIndex; index++) {
            if (buf.getByte(index) != QUOTE || !matches(buf, index + 1, writerIndex, asciiKey)) {
                continue;
            }
            int afterKey = index + 1 + asciiKey.length;
            if (afterKey >= writerIndex || buf.getByte(afterKey) != QUOTE) {
                continue;
            }
            int afterQuote = skipWhitespace(buf, afterKey + 1, writerIndex);
            if (afterQuote < writerIndex && buf.getByte(afterQuote) == COLON) {
                buf.readerIndex(afterQuote + 1);
                return true;
            }
        }
        return false;
    }

    /**
     * Copies the quoted ASCII string at the reader index into a caller-owned destination buffer.
     *
     * <p>The method expects the reader index to point at the opening quote. It
     * copies bytes until the closing quote, leaves the reader index immediately
     * after that closing quote, and returns the copied length. Escaped JSON
     * strings are not decoded in Phase 1 because venue fields that use this path
     * are expected to be simple ASCII tokens. If the value is malformed or would
     * overflow {@code dest}, the method leaves the reader index unchanged and
     * returns {@code -1}.</p>
     *
     * @param buf source buffer positioned at a quoted string
     * @param dest caller-owned destination byte array
     * @return number of bytes copied, or {@code -1} when the string is malformed or too large
     * @throws NullPointerException if {@code buf} or {@code dest} is null
     */
    public static int readStringInto(ByteBuf buf, byte[] dest) {
        Objects.requireNonNull(buf, "buf");
        Objects.requireNonNull(dest, "dest");
        int start = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        if (start >= writerIndex || buf.getByte(start) != QUOTE) {
            return -1;
        }

        int length = 0;
        for (int index = start + 1; index < writerIndex; index++) {
            byte value = buf.getByte(index);
            if (value == QUOTE) {
                buf.readerIndex(index + 1);
                return length;
            }
            if (value == '\\' || length == dest.length) {
                return -1;
            }
            dest[length++] = value;
        }
        return -1;
    }

    /**
     * Parses a quoted or unquoted fixed-point decimal at the current reader index.
     *
     * <p>The parser uses only integer arithmetic. It never calls
     * {@code Double.parseDouble}, never uses floating-point operations, and
     * retains trailing fractional zeros in the returned scale. It writes
     * {@code result[0] = mantissa} and {@code result[1] = scale}; malformed
     * input, scientific notation, scale greater than 18, or long overflow write
     * the decimal sentinel pair. Because the sentinel is also a representable
     * decimal when scale is zero, parser callers should treat that pair as
     * malformed rather than publishing it. The reader index is left at the first
     * delimiter after the numeric text, such as a closing quote, comma, bracket,
     * brace, or whitespace.</p>
     *
     * @param buf source buffer positioned at optional whitespace, optional quote, or decimal text
     * @param result caller-owned {@code long[2]} receiving mantissa and scale
     * @throws NullPointerException if {@code buf} or {@code result} is null
     * @throws IllegalArgumentException if {@code result} has fewer than two slots
     */
    public static void parseDecimal(ByteBuf buf, long[] result) {
        Objects.requireNonNull(buf, "buf");
        requireDecimalResult(result);
        int writerIndex = buf.writerIndex();
        int start = skipWhitespace(buf, buf.readerIndex(), writerIndex);
        int endPosition = parseDecimalRange(buf, start, writerIndex, result);
        buf.readerIndex(endPosition);
    }

    /**
     * Parses a Coinbase UUID into high and low 64-bit halves without allocating.
     *
     * <p>The reader index may point at an opening quote or the first UUID byte.
     * Parsing consumes exactly 16 bytes encoded as 32 hex digits while skipping
     * the four RFC 4122 hyphens. When successful, {@code result[0]} receives the
     * most significant eight bytes and {@code result[1]} receives the least
     * significant eight bytes, matching the schema v2 order-id encoding contract
     * used by L3 parsers and {@code ORDER_EVENT}/{@code TRADE_EVENT} messages.
     * On malformed or truncated input the method writes {@code 0L} to both
     * slots, leaves the reader index at the failure point, and returns
     * {@code false} instead of throwing so hot-path callers can reject the
     * message without extra error handling.</p>
     *
     * @param buf source buffer positioned at optional whitespace, an optional opening quote, or UUID text
     * @param result caller-owned {@code long[2]} receiving high and low UUID halves
     * @return {@code true} when a full UUID was parsed, otherwise {@code false}
     * @throws NullPointerException if {@code buf} or {@code result} is null
     * @throws IllegalArgumentException if {@code result} has fewer than two slots
     */
    public static boolean parseUuidHighLow(ByteBuf buf, long[] result) {
        Objects.requireNonNull(buf, "buf");
        requireUuidResult(result);

        int writerIndex = buf.writerIndex();
        int index = skipWhitespace(buf, buf.readerIndex(), writerIndex);
        boolean quoted = index < writerIndex && buf.getByte(index) == QUOTE;
        if (quoted) {
            index++;
        }

        long high = 0L;
        long low = 0L;
        int bytesParsed = 0;
        while (index < writerIndex) {
            byte current = buf.getByte(index);
            if (current == MINUS) {
                index++;
                continue;
            }
            if (quoted && current == QUOTE) {
                break;
            }

            int highNibble = hexNibble(current);
            if (highNibble < 0 || index + 1 >= writerIndex) {
                return failUuidParse(buf, result, index);
            }

            int lowNibble = hexNibble(buf.getByte(index + 1));
            if (lowNibble < 0) {
                return failUuidParse(buf, result, index + 1);
            }

            if (bytesParsed == 16) {
                return failUuidParse(buf, result, index);
            }

            long byteValue = ((highNibble << 4) | lowNibble) & 0xFFL;
            if (bytesParsed < 8) {
                high = (high << 8) | byteValue;
            } else {
                low = (low << 8) | byteValue;
            }

            bytesParsed++;
            index += 2;
            if (bytesParsed == 16) {
                break;
            }
        }

        if (bytesParsed != 16) {
            return failUuidParse(buf, result, index);
        }

        if (quoted && index < writerIndex && buf.getByte(index) == QUOTE) {
            index++;
        }

        result[0] = high;
        result[1] = low;
        buf.readerIndex(index);
        return true;
    }

    /**
     * Parses a fixed-point decimal from an explicit buffer range without changing the reader index.
     *
     * <p>This overload is useful when a venue parser has already identified a
     * JSON value range while scanning a larger frame. The same sentinel behavior
     * and integer-only parsing rules as {@link #parseDecimal(ByteBuf, long[])}
     * apply.</p>
     *
     * @param buf source buffer
     * @param start inclusive start offset
     * @param end exclusive end offset
     * @param result caller-owned {@code long[2]} receiving mantissa and scale
     * @throws NullPointerException if {@code buf} or {@code result} is null
     * @throws IllegalArgumentException if {@code result} has fewer than two slots
     * @throws IndexOutOfBoundsException if the range is outside written buffer data
     */
    public static void parseDecimal(ByteBuf buf, int start, int end, long[] result) {
        Objects.requireNonNull(buf, "buf");
        requireDecimalResult(result);
        requireRange(buf, start, end);
        parseDecimalRange(buf, skipWhitespace(buf, start, end), end, result);
    }

    /**
     * Reads an unsigned ASCII integer at the current reader index.
     *
     * <p>The reader index advances to the first non-digit byte. Overflow returns
     * {@link Long#MAX_VALUE}; callers that need strict validation should compare
     * against the sentinel and reject the message.</p>
     *
     * @param buf source buffer positioned at an ASCII digit sequence
     * @return parsed long value, or {@link Long#MAX_VALUE} on overflow
     * @throws NullPointerException if {@code buf} is null
     */
    public static long readLong(ByteBuf buf) {
        Objects.requireNonNull(buf, "buf");
        long value = 0L;
        int index = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        while (index < writerIndex) {
            byte current = buf.getByte(index);
            if (!isDigit(current)) {
                break;
            }
            int digit = current - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                buf.readerIndex(index);
                return Long.MAX_VALUE;
            }
            value = (value * 10L) + digit;
            index++;
        }
        buf.readerIndex(index);
        return value;
    }

    /**
     * Advances from a JSON array separator to the next element start.
     *
     * <p>The reader index may point at whitespace, a comma after the prior
     * element, or the closing array bracket. The method skips whitespace and one
     * optional comma. It returns {@code true} when positioned at the next
     * element and {@code false} when the array end has been reached.</p>
     *
     * @param buf buffer whose reader index is inside a JSON array
     * @return true if the reader index now points at another element, false at array end or buffer end
     * @throws NullPointerException if {@code buf} is null
     */
    public static boolean nextArrayElement(ByteBuf buf) {
        Objects.requireNonNull(buf, "buf");
        int index = skipWhitespace(buf, buf.readerIndex(), buf.writerIndex());
        if (index < buf.writerIndex() && buf.getByte(index) == COMMA) {
            index = skipWhitespace(buf, index + 1, buf.writerIndex());
        }
        if (index >= buf.writerIndex()) {
            buf.readerIndex(index);
            return false;
        }
        if (buf.getByte(index) == ARRAY_END) {
            buf.readerIndex(index + 1);
            return false;
        }
        buf.readerIndex(index);
        return true;
    }

    /**
     * Parses an RFC 3339 UTC timestamp at the current reader index to epoch nanoseconds.
     *
     * <p>The parser accepts quoted or unquoted values in the form
     * {@code yyyy-MM-ddTHH:mm:ss[.fraction]Z}. Fractional seconds may have one
     * to nine digits and are right-padded to nanoseconds. It intentionally does
     * not allocate {@code Instant}, {@code OffsetDateTime}, or
     * {@code DateTimeFormatter} objects on the hot path. Absent {@code null},
     * empty quoted values, malformed values, and epoch zero return
     * {@link #TIMESTAMP_SENTINEL}. The reader index advances to the delimiter
     * after the timestamp text.</p>
     *
     * @param buf source buffer positioned at optional whitespace, optional quote, or timestamp text
     * @return epoch nanoseconds, or {@link #TIMESTAMP_SENTINEL}
     * @throws NullPointerException if {@code buf} is null
     */
    public static long parseTimestampNanos(ByteBuf buf) {
        Objects.requireNonNull(buf, "buf");
        int writerIndex = buf.writerIndex();
        int start = skipWhitespace(buf, buf.readerIndex(), writerIndex);
        if (matchesLiteral(buf, start, writerIndex, "null")) {
            buf.readerIndex(start + 4);
            return TIMESTAMP_SENTINEL;
        }

        boolean quoted = start < writerIndex && buf.getByte(start) == QUOTE;
        int valueStart = quoted ? start + 1 : start;
        int valueEnd = valueStart;
        while (valueEnd < writerIndex) {
            byte current = buf.getByte(valueEnd);
            if (quoted ? current == QUOTE : isJsonDelimiter(current)) {
                break;
            }
            valueEnd++;
        }

        long parsed = parseRfc3339ToEpochNanos(buf, valueStart, valueEnd);
        buf.readerIndex(quoted && valueEnd < writerIndex ? valueEnd + 1 : valueEnd);
        return parsed;
    }

    /**
     * Parses an RFC 3339 UTC timestamp from an explicit buffer range without changing the reader index.
     *
     * <p>The accepted grammar and sentinel behavior match
     * {@link #parseTimestampNanos(ByteBuf)}. This overload is intended for
     * parsers that have already found a value range while scanning JSON.</p>
     *
     * @param buf source buffer
     * @param start inclusive start offset
     * @param end exclusive end offset
     * @return epoch nanoseconds, or {@link #TIMESTAMP_SENTINEL}
     * @throws NullPointerException if {@code buf} is null
     * @throws IndexOutOfBoundsException if the range is outside written buffer data
     */
    public static long parseRfc3339ToEpochNanos(ByteBuf buf, int start, int end) {
        Objects.requireNonNull(buf, "buf");
        requireRange(buf, start, end);
        int valueStart = skipWhitespace(buf, start, end);
        int valueEnd = trimTrailingWhitespace(buf, valueStart, end);
        if (valueStart >= valueEnd) {
            return TIMESTAMP_SENTINEL;
        }
        if (matchesLiteral(buf, valueStart, valueEnd, "null")) {
            return TIMESTAMP_SENTINEL;
        }
        if (buf.getByte(valueStart) == QUOTE && valueEnd > valueStart && buf.getByte(valueEnd - 1) == QUOTE) {
            valueStart++;
            valueEnd--;
            if (valueStart >= valueEnd) {
                return TIMESTAMP_SENTINEL;
            }
        }
        return parseTimestampRange(buf, valueStart, valueEnd);
    }

    /**
     * Skips a single JSON value at the current reader index.
     *
     * <p>The method handles strings, numbers, literals, arrays, and objects
     * sufficiently for parser field-skipping in Phase 1. Nested arrays and
     * objects are balanced, and quoted strings are skipped without unescaping or
     * allocating. The reader index is left after the skipped value or at the
     * writer index when the value is malformed or incomplete.</p>
     *
     * @param buf buffer positioned at a JSON value or leading whitespace
     * @throws NullPointerException if {@code buf} is null
     */
    public static void skipValue(ByteBuf buf) {
        Objects.requireNonNull(buf, "buf");
        int writerIndex = buf.writerIndex();
        int index = skipWhitespace(buf, buf.readerIndex(), writerIndex);
        if (index >= writerIndex) {
            buf.readerIndex(index);
            return;
        }

        byte first = buf.getByte(index);
        if (first == QUOTE) {
            buf.readerIndex(skipString(buf, index, writerIndex));
            return;
        }
        if (first == '[' || first == '{') {
            buf.readerIndex(skipContainer(buf, index, writerIndex));
            return;
        }

        while (index < writerIndex && !isJsonDelimiter(buf.getByte(index))) {
            index++;
        }
        buf.readerIndex(index);
    }

    private static int parseDecimalRange(ByteBuf buf, int start, int end, long[] result) {
        int index = start;
        if (index < end && buf.getByte(index) == QUOTE) {
            index++;
        }

        boolean negative = false;
        if (index < end && buf.getByte(index) == MINUS) {
            negative = true;
            index++;
        }

        long mantissa = 0L;
        int scale = 0;
        boolean sawDigit = false;
        boolean fractional = false;
        while (index < end) {
            byte current = buf.getByte(index);
            if (isDigit(current)) {
                int digit = current - '0';
                if (mantissa > (Long.MAX_VALUE - digit) / 10L) {
                    writeDecimalSentinel(result);
                    return index;
                }
                mantissa = (mantissa * 10L) + digit;
                sawDigit = true;
                if (fractional) {
                    scale++;
                    if (scale > MAX_DECIMAL_SCALE) {
                        writeDecimalSentinel(result);
                        return index;
                    }
                }
                index++;
                continue;
            }
            if (current == DECIMAL_POINT && !fractional) {
                fractional = true;
                index++;
                continue;
            }
            if (current == 'e' || current == 'E') {
                writeDecimalSentinel(result);
                return index;
            }
            if (isJsonDelimiter(current)) {
                break;
            }
            writeDecimalSentinel(result);
            return index;
        }

        if (!sawDigit) {
            writeDecimalSentinel(result);
            return index;
        }
        result[0] = negative ? -mantissa : mantissa;
        result[1] = scale;
        return index;
    }

    private static long parseTimestampRange(ByteBuf buf, int start, int end) {
        int requiredLengthWithoutFraction = 20;
        if (end - start < requiredLengthWithoutFraction) {
            return TIMESTAMP_SENTINEL;
        }
        if (buf.getByte(start + 4) != '-'
                || buf.getByte(start + 7) != '-'
                || buf.getByte(start + 10) != 'T'
                || buf.getByte(start + 13) != ':'
                || buf.getByte(start + 16) != ':') {
            return TIMESTAMP_SENTINEL;
        }

        int year = parseFixedInt(buf, start, 4);
        int month = parseFixedInt(buf, start + 5, 2);
        int day = parseFixedInt(buf, start + 8, 2);
        int hour = parseFixedInt(buf, start + 11, 2);
        int minute = parseFixedInt(buf, start + 14, 2);
        int second = parseFixedInt(buf, start + 17, 2);
        if (year < 0 || !validDateTime(year, month, day, hour, minute, second)) {
            return TIMESTAMP_SENTINEL;
        }

        int index = start + 19;
        long nanos = 0L;
        if (index < end && buf.getByte(index) == DECIMAL_POINT) {
            index++;
            int digits = 0;
            while (index < end && isDigit(buf.getByte(index))) {
                if (digits == 9) {
                    return TIMESTAMP_SENTINEL;
                }
                nanos = (nanos * 10L) + (buf.getByte(index) - '0');
                digits++;
                index++;
            }
            if (digits == 0) {
                return TIMESTAMP_SENTINEL;
            }
            while (digits < 9) {
                nanos *= 10L;
                digits++;
            }
        }
        if (index != end - 1 || buf.getByte(index) != 'Z') {
            return TIMESTAMP_SENTINEL;
        }

        long epochSecond = daysSinceEpoch(year, month, day) * SECONDS_PER_DAY
                + (hour * 3_600L)
                + (minute * 60L)
                + second;
        if (epochSecond == 0L && nanos == 0L) {
            return TIMESTAMP_SENTINEL;
        }
        if (epochSecond > (Long.MAX_VALUE - nanos) / NANOS_PER_SECOND) {
            return TIMESTAMP_SENTINEL;
        }
        return (epochSecond * NANOS_PER_SECOND) + nanos;
    }

    private static int scanExact(ByteBuf buf, int start, int end, byte[] pattern) {
        int lastStart = end - pattern.length;
        for (int index = start; index <= lastStart; index++) {
            if (matches(buf, index, end, pattern)) {
                return index + pattern.length;
            }
        }
        return -1;
    }

    private static boolean matches(ByteBuf buf, int start, int end, byte[] pattern) {
        if (start + pattern.length > end) {
            return false;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (buf.getByte(start + i) != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesLiteral(ByteBuf buf, int start, int end, String literal) {
        if (start + literal.length() > end) {
            return false;
        }
        for (int i = 0; i < literal.length(); i++) {
            if (buf.getByte(start + i) != literal.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static int parseFixedInt(ByteBuf buf, int start, int length) {
        int value = 0;
        for (int i = 0; i < length; i++) {
            byte current = buf.getByte(start + i);
            if (!isDigit(current)) {
                return -1;
            }
            value = (value * 10) + (current - '0');
        }
        return value;
    }

    private static int hexNibble(byte value) {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return (value - 'a') + 10;
        }
        if (value >= 'A' && value <= 'F') {
            return (value - 'A') + 10;
        }
        return -1;
    }

    private static long daysSinceEpoch(int year, int month, int day) {
        int adjustedYear = year - (month <= 2 ? 1 : 0);
        long era = Math.floorDiv(adjustedYear, 400);
        int yearOfEra = adjustedYear - (int) (era * 400);
        int monthPrime = month > 2 ? month - 3 : month + 9;
        int dayOfYear = ((153 * monthPrime) + 2) / 5 + day - 1;
        int dayOfEra = (yearOfEra * 365) + (yearOfEra / 4) - (yearOfEra / 100) + dayOfYear;
        return (era * 146_097L) + dayOfEra - 719_468L;
    }

    private static boolean validDateTime(int year, int month, int day, int hour, int minute, int second) {
        if (month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23
                || minute < 0 || minute > 59 || second < 0 || second > 59) {
            return false;
        }
        return day <= daysInMonth(year, month);
    }

    private static int daysInMonth(int year, int month) {
        return switch (month) {
            case 2 -> isLeapYear(year) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            default -> 31;
        };
    }

    private static boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    }

    private static int skipString(ByteBuf buf, int start, int end) {
        int index = start + 1;
        while (index < end) {
            byte current = buf.getByte(index);
            if (current == '\\') {
                index += 2;
                continue;
            }
            if (current == QUOTE) {
                return index + 1;
            }
            index++;
        }
        return end;
    }

    private static int skipContainer(ByteBuf buf, int start, int end) {
        int depth = 0;
        int index = start;
        while (index < end) {
            byte current = buf.getByte(index);
            if (current == QUOTE) {
                index = skipString(buf, index, end);
                continue;
            }
            if (current == '[' || current == '{') {
                depth++;
            } else if (current == ARRAY_END || current == OBJECT_END) {
                depth--;
                if (depth == 0) {
                    return index + 1;
                }
            }
            index++;
        }
        return end;
    }

    private static int skipWhitespace(ByteBuf buf, int start, int end) {
        int index = start;
        while (index < end && isWhitespace(buf.getByte(index))) {
            index++;
        }
        return index;
    }

    private static int trimTrailingWhitespace(ByteBuf buf, int start, int end) {
        int index = end;
        while (index > start && isWhitespace(buf.getByte(index - 1))) {
            index--;
        }
        return index;
    }

    private static boolean isJsonDelimiter(byte value) {
        return value == QUOTE
                || value == COMMA
                || value == ARRAY_END
                || value == OBJECT_END
                || isWhitespace(value);
    }

    private static boolean isDigit(byte value) {
        return value >= '0' && value <= '9';
    }

    private static boolean isWhitespace(byte value) {
        return value == ' ' || value == '\n' || value == '\r' || value == '\t';
    }

    private static void writeDecimalSentinel(long[] result) {
        result[0] = DECIMAL_SENTINEL_MANTISSA;
        result[1] = DECIMAL_SENTINEL_SCALE;
    }

    private static void requireDecimalResult(long[] result) {
        Objects.requireNonNull(result, "result");
        if (result.length < 2) {
            throw new IllegalArgumentException("result must have at least two slots");
        }
    }

    private static void requireUuidResult(long[] result) {
        Objects.requireNonNull(result, "result");
        if (result.length < 2) {
            throw new IllegalArgumentException("result must have at least two slots");
        }
    }

    private static boolean failUuidParse(ByteBuf buf, long[] result, int readerIndex) {
        result[0] = 0L;
        result[1] = 0L;
        buf.readerIndex(readerIndex);
        return false;
    }

    private static void requireRange(ByteBuf buf, int start, int end) {
        if (start < 0 || end < start || end > buf.writerIndex()) {
            throw new IndexOutOfBoundsException(
                    "range [" + start + ", " + end + ") is outside writerIndex " + buf.writerIndex());
        }
    }
}
