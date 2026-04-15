package io.rueishi.marketdata.crypto.core.publisher;

import io.rueishi.marketdata.crypto.core.config.LoggingPublisherConfig;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.async.AsyncLoggerContext;
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

/**
 * Log4j 2 asynchronous logging {@link Publisher} implementation.
 *
 * <p>{@code LoggingPublisher} copies the encoder-owned SBE bytes into a Base64
 * log record, then hands that immutable string to a dedicated Log4j 2 async
 * logger context. Log4j's async logger context is backed by the LMAX Disruptor,
 * so event-loop threads avoid file I/O while still satisfying the publisher
 * buffer ownership contract.</p>
 *
 * <p>Connectors call this object through {@link Publisher#publish} after
 * parser-owned encoding completes. {@link #publishReset(int, byte, byte, byte,
 * NanoClock)} encodes the same zero-entry reset prefix used by other reset
 * publishers and logs it without updating publisher-stage latency counters.</p>
 */
public final class LoggingPublisher implements Publisher, AutoCloseable {
    private static final String ASYNC_CONTEXT_SELECTOR_PROPERTY = "Log4jContextSelector";
    private static final String LOGGER_NAME = "io.rueishi.marketdata.crypto.publisher.messages";
    private static final String LOG_LINE_PREFIX = "sbe_base64=";
    private static int contextSequence;

    private final UnsafeBuffer resetEncodingBuffer = new UnsafeBuffer(new byte[EncodingConstants.BOOK_RESET_SIZE]);
    private final LoggerContext loggerContext;
    private final Logger logger;

    /**
     * Creates a synchronous logging publisher boundary.
     */
    public LoggingPublisher() {
        this(defaultConfig());
    }

    /**
     * Creates an asynchronous rolling-file logging publisher.
     *
     * @param config logging publisher configuration
     * @throws NullPointerException if {@code config} or {@code outputPath} is null
     * @throws IllegalArgumentException if {@code rollSizeMb} is not positive
     */
    public LoggingPublisher(LoggingPublisherConfig config) {
        Objects.requireNonNull(config, "config");
        String outputPath = Objects.requireNonNull(config.outputPath, "config.outputPath");
        if (config.rollSizeMb <= 0) {
            throw new IllegalArgumentException("config.rollSizeMb must be positive");
        }
        createParentDirectories(outputPath);
        ensureAsyncLoggerContextSelector();
        loggerContext = new AsyncLoggerContext(nextContextName());
        loggerContext.start(buildConfiguration(outputPath, config.rollSizeMb));
        logger = loggerContext.getLogger(LOGGER_NAME);
    }

    /**
     * Queues one encoded message for asynchronous file logging and records publisher-stage latency.
     *
     * <p>The implementation copies and Base64-encodes the requested region
     * before returning. Log4j owns the resulting string passed to the async
     * logger, so later encoder writes cannot corrupt queued log events.</p>
     *
     * @param buffer encoder-owned reusable buffer
     * @param offset first byte to consume
     * @param length number of bytes to consume
     * @param counters per-instrument counters, or null when tests do not track metrics
     * @param nanoClock caller-owned clock, or null when latency counters are not being tested
     * @return always {@code true}
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if the region is invalid
     */
    @Override
    public boolean publish(DirectBuffer buffer, int offset, int length, InstrumentCounters counters, NanoClock nanoClock) {
        validateRegion(buffer, offset, length);
        long startNanos = nanoClock == null ? 0L : nanoClock.nanoTime();
        logger.info(LOG_LINE_PREFIX + encodedPayload(buffer, offset, length));
        if (counters != null) {
            counters.messagesPublished().increment();
            if (nanoClock != null) {
                PublisherLatencyStats.record(counters, nanoClock.nanoTime() - startNanos);
            }
        }
        return true;
    }

    /**
     * Accepts a reset control message without updating publisher-stage latency.
     *
     * <p>The reset message has no repeating group, uses sequence fields of
     * {@code 0}, stores {@code -1} for exchange timestamp, writes the schema v2
     * checksum placeholder as {@code 0}, and uses {@code nanoClock.nanoTime()}
     * for ingress timestamp.</p>
     *
     * @param instrumentId stable internal instrument id for the reset
     * @param venueByte encoded venue byte
     * @param bookDepthByte encoded book-depth byte
     * @param templateIdByte encoded template byte
     * @param nanoClock caller-owned clock used by reset-capable publishers
     * @throws NullPointerException if {@code nanoClock} is null
     */
    @Override
    public void publishReset(int instrumentId, byte venueByte, byte bookDepthByte, byte templateIdByte, NanoClock nanoClock) {
        long ingressTimestamp = Objects.requireNonNull(nanoClock, "nanoClock").nanoTime();
        resetEncodingBuffer.putShort(
                EncodingConstants.MAGIC_OFFSET,
                (short) EncodingConstants.MAGIC,
                EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putByte(EncodingConstants.VERSION_OFFSET, (byte) EncodingConstants.VERSION);
        resetEncodingBuffer.putByte(EncodingConstants.TEMPLATE_ID_OFFSET, templateIdByte);
        resetEncodingBuffer.putShort(
                EncodingConstants.BLOCK_LENGTH_OFFSET,
                (short) EncodingConstants.BODY_BLOCK_LENGTH,
                EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putShort(EncodingConstants.ENTRY_COUNT_OFFSET, (short) 0, EncodingConstants.BYTE_ORDER);

        resetEncodingBuffer.putByte(EncodingConstants.EVENT_TYPE_OFFSET, EncodingConstants.EVENT_TYPE_BOOK_RESET);
        resetEncodingBuffer.putByte(EncodingConstants.VENUE_OFFSET, venueByte);
        resetEncodingBuffer.putByte(EncodingConstants.BOOK_DEPTH_OFFSET, bookDepthByte);
        resetEncodingBuffer.putInt(EncodingConstants.INSTRUMENT_ID_OFFSET, instrumentId, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.SEQ1_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.SEQ2_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(
                EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET,
                EncodingConstants.NO_TIMESTAMP,
                EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET, ingressTimestamp, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putInt(EncodingConstants.CHECKSUM_OFFSET, EncodingConstants.NO_CHECKSUM, EncodingConstants.BYTE_ORDER);

        logger.info(LOG_LINE_PREFIX + encodedPayload(resetEncodingBuffer, 0, EncodingConstants.BOOK_RESET_SIZE));
    }

    /**
     * Legacy placeholder guard retained for older smoke tests.
     *
     * @throws UnsupportedOperationException always; callers should use {@link #publish}
     */
    public void publishUnsupported() {
        throw new UnsupportedOperationException(
                "LoggingPublisher behavior is no longer deferred beyond P1-000a; use Publisher.publish");
    }

    /**
     * Flushes queued async log events and stops the dedicated Log4j context.
     */
    @Override
    public void close() {
        Configurator.shutdown(loggerContext);
    }

    private static BuiltConfiguration buildConfiguration(String outputPath, int rollSizeMb) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setConfigurationName("gateway-logging-publisher");
        builder.setStatusLevel(org.apache.logging.log4j.Level.ERROR);

        LayoutComponentBuilder layout = builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%m%n");
        ComponentBuilder<?> policy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy")
                        .addAttribute("size", rollSizeMb + " MB"));
        AppenderComponentBuilder appender = builder.newAppender("SbeRollingFile", "RollingRandomAccessFile")
                .addAttribute("fileName", outputPath)
                .addAttribute("filePattern", rolloverPattern(outputPath))
                .add(layout)
                .addComponent(policy);
        builder.add(appender);
        builder.add(builder.newLogger(LOGGER_NAME, org.apache.logging.log4j.Level.INFO)
                .add(builder.newAppenderRef("SbeRollingFile"))
                .addAttribute("additivity", false));
        return builder.build();
    }

    private static String rolloverPattern(String outputPath) {
        Path path = Path.of(outputPath);
        Path fileName = path.getFileName();
        String rolledName = fileName == null ? "gateway-messages-%i.log.gz" : fileName + ".%i.gz";
        Path parent = path.getParent();
        return parent == null ? rolledName : parent.resolve(rolledName).toString();
    }

    private static void createParentDirectories(String outputPath) {
        Path parent = Path.of(outputPath).getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to create logging publisher directory " + parent, ex);
            }
        }
    }

    private static LoggingPublisherConfig defaultConfig() {
        LoggingPublisherConfig config = new LoggingPublisherConfig();
        config.outputPath = "var/run/logging-publisher/messages.log";
        config.rollSizeMb = 256;
        return config;
    }

    private static void ensureAsyncLoggerContextSelector() {
        System.setProperty(ASYNC_CONTEXT_SELECTOR_PROPERTY, AsyncLoggerContextSelector.class.getName());
    }

    private static synchronized String nextContextName() {
        return "gateway-logging-publisher-" + contextSequence++;
    }

    private static String encodedPayload(DirectBuffer buffer, int offset, int length) {
        byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        return Base64.getEncoder().encodeToString(copy);
    }

    private static void validateRegion(DirectBuffer buffer, int offset, int length) {
        Objects.requireNonNull(buffer, "buffer");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        if (offset > buffer.capacity() - length) {
            throw new IllegalArgumentException("buffer region is out of bounds");
        }
    }
}
