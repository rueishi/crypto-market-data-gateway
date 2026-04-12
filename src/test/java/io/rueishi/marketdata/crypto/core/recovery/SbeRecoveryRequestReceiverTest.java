package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SbeRecoveryRequestReceiver}.
 *
 * <p>The tests use recording connector stubs and an injectable receiver
 * constructor instead of a full {@link io.rueishi.marketdata.crypto.core.bootstrap.GatewayRuntime}.
 * This isolates downstream control-message behavior while still verifying the
 * production receiver's decode, route, rejection, and result-classification
 * logic.</p>
 */
class SbeRecoveryRequestReceiverTest {

    /**
     * Verifies a decoded request is delegated to the supplied runtime recovery function.
     */
    @Test
    void routesDecodedSingleRequest() {
        RecordingRoute route = new RecordingRoute(true);
        SbeRecoveryRequestReceiver receiver = receiver(route, List.of(new RecordingConnector(1001)));
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        int length = new SbeRecoveryRequestEncoder().encodeRequest(buffer, 0, new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123L));

        RecoveryRequestBatchResult result = receiver.receive(buffer, 0, length);

        assertThat(result.decodedRequests()).isEqualTo(1);
        assertThat(result.acceptedRequests()).isEqualTo(1);
        assertThat(route.calls).hasValue(1);
        assertThat(route.lastRequest.get().instrumentId).isEqualTo(1001);
    }

    /**
     * Verifies batch messages route each instrument independently and report unknown instruments.
     */
    @Test
    void routesBatchAndClassifiesUnknownInstrument() {
        RecordingRoute route = new RecordingRoute(false);
        SbeRecoveryRequestReceiver receiver = receiver(route, List.of(new RecordingConnector(1001)));
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        int length = new SbeRecoveryRequestEncoder().encodeBatch(
                buffer,
                0,
                VenueEnum.COINBASE_L2,
                RecoveryRequestType.RESYNC,
                RecoveryReasonCode.CHECK_FAILED,
                456L,
                new int[] {1001, 9999});

        RecoveryRequestBatchResult result = receiver.receive(buffer, 0, length);

        assertThat(result.decodedRequests()).isEqualTo(2);
        assertThat(result.acceptedRequests()).isZero();
        assertThat(result.rejectedRequests()).isEqualTo(1);
        assertThat(result.unknownInstrumentRequests()).isEqualTo(1);
        assertThat(route.calls).hasValue(2);
    }

    /**
     * Verifies malformed messages and market-data BOOK_RESET payloads do not invoke recovery routing.
     */
    @Test
    void rejectsMalformedAndBookResetWithoutRouting() {
        RecordingRoute route = new RecordingRoute(true);
        SbeRecoveryRequestReceiver receiver = receiver(route, List.of(new RecordingConnector(1001)));
        UnsafeBuffer malformed = new UnsafeBuffer(new byte[4]);

        RecoveryRequestBatchResult malformedResult = receiver.receive(malformed, 0, malformed.capacity());

        assertThat(malformedResult.malformedMessages()).isEqualTo(1);
        assertThat(route.calls).hasValue(0);

        UnsafeBuffer bookReset = new UnsafeBuffer(new byte[EncodingConstants.MESSAGE_PREFIX_LENGTH]);
        bookReset.putShort(EncodingConstants.MAGIC_OFFSET, (short) EncodingConstants.MAGIC, EncodingConstants.BYTE_ORDER);
        bookReset.putByte(EncodingConstants.VERSION_OFFSET, (byte) EncodingConstants.VERSION);
        bookReset.putByte(EncodingConstants.TEMPLATE_ID_OFFSET, VenueEnum.COINBASE_L2.templateId().byteValue());
        bookReset.putShort(
                EncodingConstants.BLOCK_LENGTH_OFFSET,
                (short) EncodingConstants.BODY_BLOCK_LENGTH,
                EncodingConstants.BYTE_ORDER);
        bookReset.putByte(EncodingConstants.EVENT_TYPE_OFFSET, EncodingConstants.EVENT_TYPE_BOOK_RESET);

        RecoveryRequestBatchResult bookResetResult =
                receiver.receive(bookReset, 0, EncodingConstants.MESSAGE_PREFIX_LENGTH);

        assertThat(bookResetResult.malformedMessages()).isEqualTo(1);
        assertThat(route.calls).hasValue(0);
    }

    private static SbeRecoveryRequestReceiver receiver(RecordingRoute route, List<Connector> connectors) {
        return new SbeRecoveryRequestReceiver(
                route::route,
                () -> VenueEnum.COINBASE_L2,
                () -> connectors,
                new SbeRecoveryRequestDecoder());
    }

    private static final class RecordingRoute {
        private final boolean accepted;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<RecoveryRequest> lastRequest = new AtomicReference<>();

        private RecordingRoute(boolean accepted) {
            this.accepted = accepted;
        }

        private boolean route(RecoveryRequest request) {
            calls.incrementAndGet();
            lastRequest.set(request);
            return accepted;
        }
    }

    private record RecordingConnector(int instrumentId) implements Connector {
        @Override
        public void init(ConnectorContext ctx) {
        }

        @Override
        public void connect() {
        }

        @Override
        public void recover(RecoveryRequest request) {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }
    }
}
