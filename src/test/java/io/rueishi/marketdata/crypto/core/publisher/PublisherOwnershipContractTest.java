package io.rueishi.marketdata.crypto.core.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Ownership-contract tests for {@link Publisher} implementations.
 *
 * <p>These tests drive {@link InMemoryPublisher} through the real
 * {@link SbeEncoder} reusable direct buffer. They prove the publisher copies
 * bytes before returning: a later encoder write to the same buffer does not
 * mutate a previously captured message returned by the publisher. A test-only
 * async publisher covers the same contract for queued handoff behavior without
 * adding a production async adapter.</p>
 */
class PublisherOwnershipContractTest {

    /**
     * Verifies that InMemoryPublisher does not retain the encoder's reusable buffer reference.
     */
    @Test
    void synchronousPublisherDoesNotRetainEncoderBufferReference() {
        InMemoryPublisher publisher = new InMemoryPublisher(4, 256);
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 2, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 10L);
        encoder.writeLevel((byte) 1, (byte) 1, 100L, (byte) 1, 200L, (byte) 2);
        encoder.endMessage(publisher, null, null);
        byte[] firstSnapshot = publisher.lastMessage();

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 2, 2, 2, -1L, 20L);
        encoder.writeLevel((byte) 2, (byte) 2, 300L, (byte) 3, 400L, (byte) 4);
        encoder.endMessage(publisher, null, null);

        assertThat(firstSnapshot[EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET]).isEqualTo((byte) 1);
        assertThat(firstSnapshot).isNotEqualTo(publisher.lastMessage());
        assertThat(publisher.allMessages()).hasSize(2);
        assertThat(publisher.allMessages().get(0)).isEqualTo(firstSnapshot);
    }

    /**
     * Verifies high-volume capture uses the configured bounded ring instead of append-only growth.
     */
    @Test
    void highVolumePublishLoopRetainsOnlyConfiguredRingCapacity() {
        InMemoryPublisher publisher = new InMemoryPublisher(8, 256);
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        for (int i = 1; i <= 1_000; i++) {
            encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, i, i, i, -1L, i);
            encoder.endMessage(publisher, null, null);
        }

        assertThat(publisher.totalCount()).isEqualTo(1_000);
        assertThat(publisher.allMessages()).hasSize(8);
    }

    /**
     * Verifies an asynchronous publisher copies bytes before returning from publish.
     */
    @Test
    void asyncPublisherCopiesEncoderBufferBeforeReturning() {
        AsyncCopyingPublisher publisher = new AsyncCopyingPublisher();
        SbeEncoder encoder = new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 1, 0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1, 1, 1, -1L, 10L);
        encoder.writeLevel((byte) 1, (byte) 1, 100L, (byte) 1, 200L, (byte) 2);
        encoder.endMessage(publisher, null, null);
        byte[] firstQueuedMessage = publisher.queuedMessages.get(0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 2, 2, 2, -1L, 20L);
        encoder.writeLevel((byte) 2, (byte) 2, 300L, (byte) 3, 400L, (byte) 4);
        encoder.endMessage(publisher, null, null);

        assertThat(firstQueuedMessage[EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET]).isEqualTo((byte) 1);
        assertThat(firstQueuedMessage).isNotEqualTo(publisher.queuedMessages.get(1));
    }

    private static final class AsyncCopyingPublisher implements Publisher {
        private final List<byte[]> queuedMessages = new ArrayList<>();

        @Override
        public boolean publish(
                DirectBuffer buffer,
                int offset,
                int length,
                InstrumentCounters counters,
                NanoClock nanoClock) {
            byte[] copy = new byte[length];
            buffer.getBytes(offset, copy, 0, length);
            queuedMessages.add(copy);
            return true;
        }

        @Override
        public void publishReset(
                int instrumentId,
                byte venueByte,
                byte bookDepthByte,
                byte templateIdByte,
                NanoClock nanoClock) {
        }
    }
}
