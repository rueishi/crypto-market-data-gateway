package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReconnectRecoveryStrategy}.
 *
 * <p>These tests use concrete test subclasses to verify the base reconnect
 * template. They cover best-effort unsubscribe, ordered disconnect/reconnect/
 * resubscribe flow, the success callback, the failure callback, and the
 * counter updates on the supplied {@link RecoveryContext}.</p>
 */
class ReconnectRecoveryStrategyTest {

    /**
     * Verifies that a successful reconnect template emits one channel-restored callback.
     */
    @Test
    void executeCallsOnChannelRestoredExactlyOnceOnSuccess() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        RecordingRecoveryStrategy strategy = new RecordingRecoveryStrategy();

        strategy.execute(newRecoveryRequest(), context);

        assertThat(strategy.steps).containsExactly("unsubscribe", "disconnect", "reconnect", "resubscribe");
        assertThat(restoredCount).hasValue(1);
        assertThat(failureReason).hasValue(null);
        assertThat(counters.recoveryExecutions().get()).isEqualTo(1);
        assertThat(counters.recoveryFailures().get()).isZero();
    }

    /**
     * Verifies that an exception from a template step becomes one recovery-failed callback.
     */
    @Test
    void executeCallsOnRecoveryFailedExactlyOnceWhenTemplateStepThrows() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        RecordingRecoveryStrategy strategy = new RecordingRecoveryStrategy();
        strategy.throwOnReconnect = true;

        strategy.execute(newRecoveryRequest(), context);

        assertThat(strategy.steps).containsExactly("unsubscribe", "disconnect", "reconnect");
        assertThat(restoredCount).hasValue(0);
        assertThat(failureReason).hasValue("reconnect failed");
        assertThat(counters.recoveryExecutions().get()).isEqualTo(1);
        assertThat(counters.recoveryFailures().get()).isEqualTo(1);
    }

    /**
     * Verifies an unsubscribe failure is suppressed so recovery can still close,
     * reconnect, resubscribe, and enter the snapshot phase.
     */
    @Test
    void executeTreatsUnsubscribeAsBestEffort() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        RecordingRecoveryStrategy strategy = new RecordingRecoveryStrategy();
        strategy.throwOnUnsubscribe = true;

        strategy.execute(newRecoveryRequest(), context);

        assertThat(strategy.steps).containsExactly("unsubscribe", "disconnect", "reconnect", "resubscribe");
        assertThat(restoredCount).hasValue(1);
        assertThat(failureReason).hasValue(null);
        assertThat(strategy.bestEffortFailures).hasValue(1);
        assertThat(counters.recoveryFailures().get()).isZero();
    }

    /**
     * Verifies that an exception without a message still produces one deterministic failure reason.
     */
    @Test
    void executeUsesExceptionTypeWhenFailureMessageIsBlank() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        RecordingRecoveryStrategy strategy = new RecordingRecoveryStrategy();
        strategy.throwBlankOnDisconnect = true;

        strategy.execute(newRecoveryRequest(), context);

        assertThat(strategy.steps).containsExactly("unsubscribe", "disconnect");
        assertThat(restoredCount).hasValue(0);
        assertThat(failureReason).hasValue("IllegalStateException");
        assertThat(counters.recoveryFailures().get()).isEqualTo(1);
    }

    /**
     * Verifies that recovery request metadata retains request type, reason, and optional diagnostics.
     */
    @Test
    void recoveryRequestCarriesImmutableMetadata() {
        RecoveryRequest request = new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESYNC,
                RecoveryReasonCode.CHECK_FAILED,
                987_654_321L,
                "checksum mismatch");

        assertThat(request.venue).isEqualTo(VenueEnum.COINBASE_L2);
        assertThat(request.instrumentId).isEqualTo(1001);
        assertThat(request.requestType).isEqualTo(RecoveryRequestType.RESYNC);
        assertThat(request.reasonCode).isEqualTo(RecoveryReasonCode.CHECK_FAILED);
        assertThat(request.requestTimestamp).isEqualTo(987_654_321L);
        assertThat(request.diagnosticText).isEqualTo("checksum mismatch");
    }

    private static RecoveryRequest newRecoveryRequest() {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                123L);
    }

    private static final class RecordingRecoveryStrategy extends ReconnectRecoveryStrategy {
        private final List<String> steps = new ArrayList<>();
        private final AtomicInteger bestEffortFailures = new AtomicInteger();
        private boolean throwOnUnsubscribe;
        private boolean throwOnReconnect;
        private boolean throwBlankOnDisconnect;

        @Override
        protected void doUnsubscribe() {
            steps.add("unsubscribe");
            if (throwOnUnsubscribe) {
                throw new IllegalStateException("unsubscribe failed");
            }
        }

        @Override
        protected void doDisconnect() {
            steps.add("disconnect");
            if (throwBlankOnDisconnect) {
                throw new IllegalStateException();
            }
        }

        @Override
        protected void doReconnect() {
            steps.add("reconnect");
            if (throwOnReconnect) {
                throw new IllegalStateException("reconnect failed");
            }
        }

        @Override
        protected void doResubscribe() {
            steps.add("resubscribe");
        }

        @Override
        protected void onBestEffortUnsubscribeFailure(
                RecoveryRequest request,
                RecoveryContext ctx,
                Exception ex) {
            bestEffortFailures.incrementAndGet();
        }
    }
}
