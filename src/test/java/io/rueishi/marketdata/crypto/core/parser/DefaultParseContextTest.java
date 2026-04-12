package io.rueishi.marketdata.crypto.core.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultParseContext}.
 *
 * <p>These tests verify the parser-facing context lifecycle owned by P1-005:
 * fixed dependencies remain stable for the connector lifetime, while
 * {@link #resetSession()} renews only the session-scoped sequence tracker and
 * snapshot gatekeeper. The tests also cover parser-routed snapshot boundary
 * and recovery callbacks used by subscribe-driven venues. Dependencies are
 * simple test doubles or in-memory Agrona counters; no connector or venue
 * implementation is involved.</p>
 */
class DefaultParseContextTest {

    /**
     * Verifies that resetting the parse context preserves its object identity and fixed dependencies.
     */
    @Test
    void resetSessionKeepsParseContextIdentityAndFixedDependencies() {
        Fixtures fixtures = new Fixtures();
        DefaultParseContext context = fixtures.newParseContext();
        DefaultParseContext sameContext = context;

        context.sequenceTracker().next();
        context.snapshotGatekeeper().accept();
        SbeEncoder encoder = context.encoder();
        Publisher publisher = context.publisher();
        InstrumentCounters counters = context.counters();

        context.resetSession();

        assertThat(context).isSameAs(sameContext);
        assertThat(context.encoder()).isSameAs(encoder);
        assertThat(context.publisher()).isSameAs(publisher);
        assertThat(context.counters()).isSameAs(counters);
    }

    /**
     * Verifies that resetSession renews sequence and snapshot state for a new connector session.
     */
    @Test
    void resetSessionReplacesSequenceTrackerAndSnapshotGatekeeper() {
        Fixtures fixtures = new Fixtures();
        DefaultParseContext context = fixtures.newParseContext();
        Object firstTracker = context.sequenceTracker();
        Object firstGatekeeper = context.snapshotGatekeeper();

        context.sequenceTracker().next();
        context.snapshotGatekeeper().accept();
        context.onSubscriptionAckValidated();
        context.resetSession();

        assertThat(context.sequenceTracker()).isNotSameAs(firstTracker);
        assertThat(context.sequenceTracker().current()).isZero();
        assertThat(context.sequenceTracker().next()).isEqualTo(1);
        assertThat(context.snapshotGatekeeper()).isNotSameAs(firstGatekeeper);
        assertThat(context.snapshotGatekeeper().isReady()).isFalse();
        assertThat(context.currentSnapshotGatekeeper()).isSameAs(context.snapshotGatekeeper());
        assertThat(context.subscriptionAckValidated()).isFalse();
    }

    /**
     * Verifies that subscription acknowledgement validation seeds liveness timing for the current session.
     */
    @Test
    void subscriptionAckValidationMarksSessionAndSeedsHeartbeatClock() {
        Fixtures fixtures = new Fixtures();
        DefaultParseContext context = fixtures.newParseContext();

        context.onSubscriptionAckValidated();

        assertThat(context.subscriptionAckValidated()).isTrue();
        assertThat(context.counters().lastHeartbeatReceivedNanos().get()).isEqualTo(123L);
    }

    /**
     * Verifies that subscribe-driven snapshot boundary callbacks are one-shot per parser session.
     */
    @Test
    void snapshotBoundaryAcceptedCallbackIsOneShotPerSession() {
        Fixtures fixtures = new Fixtures();
        AtomicInteger acceptedCount = new AtomicInteger();
        DefaultParseContext context = new DefaultParseContext(
                fixtures.encoder,
                fixtures.publisher,
                fixtures.counters,
                () -> 123L,
                acceptedCount::incrementAndGet);

        context.snapshotGatekeeper().accept();
        context.onSnapshotBoundaryAccepted();

        assertThat(acceptedCount).hasValue(1);
        assertThatThrownBy(context::onSnapshotBoundaryAccepted)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already accepted");

        context.resetSession();
        context.snapshotGatekeeper().accept();
        context.onSnapshotBoundaryAccepted();
        assertThat(acceptedCount).hasValue(2);
    }

    /**
     * Verifies parser-originated recovery metadata is delegated to the connector callback.
     */
    @Test
    void requestRecoveryDelegatesToConnectorCallback() {
        Fixtures fixtures = new Fixtures();
        AtomicInteger recoveryCount = new AtomicInteger();
        DefaultParseContext context = new DefaultParseContext(
                fixtures.encoder,
                fixtures.publisher,
                fixtures.counters,
                () -> 123L,
                (type, reason, diagnosticText) -> {
                    assertThat(type).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(reason).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                    assertThat(diagnosticText).isEqualTo("bad ack");
                    recoveryCount.incrementAndGet();
                },
                () -> {
                },
                () -> {
                });

        context.requestRecovery(
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                "bad ack");

        assertThat(recoveryCount).hasValue(1);
    }

    public static final class Fixtures {
        public final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        final SbeEncoder encoder = CoreTestFixtures.newEncoder();
        final Publisher publisher = CoreTestFixtures.noopPublisher();

        public DefaultParseContext newParseContext() {
            return new DefaultParseContext(encoder, publisher, counters, () -> 123L);
        }
    }
}
