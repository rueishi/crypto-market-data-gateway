package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.recovery.DefaultRecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CoinbaseL2RecoveryStrategy}.
 *
 * <p>The suite uses fake connector-owned {@link CoinbaseL2RecoveryStrategy.Actions}
 * and real {@link DefaultRecoveryContext} instances with in-memory counters.
 * It verifies the Phase 3 Coinbase recovery sequence, convergence of all
 * request types onto the same full reconnect path, best-effort unsubscribe,
 * exponential backoff with jitter between reconnect attempts, and failure
 * signaling when the retry budget is exhausted.</p>
 */
class CoinbaseL2RecoveryStrategyTest {

    /**
     * Verifies the successful Phase A order: unsubscribe, close, reconnect,
     * resubscribe, and then channel-restored callback.
     */
    @Test
    void executePerformsFullReconnectSequence() {
        Fixture fixture = new Fixture();

        fixture.strategy.execute(newRecoveryRequest(RecoveryRequestType.RESET), fixture.context);

        assertThat(fixture.actions.steps).containsExactly("unsubscribe", "close", "reconnect", "resubscribe");
        assertThat(fixture.restoredCount).hasValue(1);
        assertThat(fixture.failureReason).hasValue(null);
        assertThat(fixture.counters.recoveryExecutions().get()).isEqualTo(1);
        assertThat(fixture.counters.recoveryFailures().get()).isZero();
    }

    /**
     * Verifies RESET, RESNAPSHOT, and RESYNC use the same Coinbase full
     * reconnect path because Coinbase L2 has no snapshot-on-demand request.
     */
    @Test
    void allRequestTypesUseSameReconnectPath() {
        for (RecoveryRequestType requestType : RecoveryRequestType.values()) {
            Fixture fixture = new Fixture();

            fixture.strategy.execute(newRecoveryRequest(requestType), fixture.context);

            assertThat(fixture.actions.steps).containsExactly("unsubscribe", "close", "reconnect", "resubscribe");
            assertThat(fixture.restoredCount).hasValue(1);
        }
    }

    /**
     * Verifies unsubscribe is best-effort: a failed unsubscribe does not stop
     * close/reconnect/resubscribe or mark the recovery as failed.
     */
    @Test
    void unsubscribeFailureIsBestEffort() {
        Fixture fixture = new Fixture();
        fixture.actions.throwOnUnsubscribe = true;

        fixture.strategy.execute(newRecoveryRequest(RecoveryRequestType.RESET), fixture.context);

        assertThat(fixture.actions.steps).containsExactly("unsubscribe", "close", "reconnect", "resubscribe");
        assertThat(fixture.restoredCount).hasValue(1);
        assertThat(fixture.counters.recoveryFailures().get()).isZero();
    }

    /**
     * Verifies reconnect retries use exponential backoff plus deterministic jitter and reset after success.
     */
    @Test
    void reconnectRetriesUseExponentialBackoffWithJitter() {
        Fixture fixture = new Fixture();
        fixture.actions.reconnectFailuresRemaining = 2;

        fixture.strategy.execute(newRecoveryRequest(RecoveryRequestType.RESET), fixture.context);

        assertThat(fixture.actions.steps).containsExactly(
                "unsubscribe", "close", "reconnect", "reconnect", "reconnect", "resubscribe");
        assertThat(fixture.sleeps).containsExactly(15L, 25L);
        assertThat(fixture.restoredCount).hasValue(1);
        assertThat(fixture.counters.recoveryFailures().get()).isZero();
    }

    /**
     * Verifies exhausted reconnect retries fail the recovery attempt without
     * falsely entering the snapshot phase.
     */
    @Test
    void reconnectFailureAfterRetryBudgetSignalsRecoveryFailure() {
        Fixture fixture = new Fixture();
        fixture.actions.reconnectFailuresRemaining = 3;

        fixture.strategy.execute(newRecoveryRequest(RecoveryRequestType.RESET), fixture.context);

        assertThat(fixture.actions.steps).containsExactly(
                "unsubscribe", "close", "reconnect", "reconnect", "reconnect");
        assertThat(fixture.sleeps).containsExactly(15L, 25L);
        assertThat(fixture.restoredCount).hasValue(0);
        assertThat(fixture.failureReason).hasValue("reconnect failed");
        assertThat(fixture.counters.recoveryFailures().get()).isEqualTo(1);
    }

    /**
     * Verifies invalid backoff configuration is rejected during strategy construction.
     */
    @Test
    void rejectsInvalidBackoffConfiguration() {
        TransportConfig config = validTransportConfig();
        config.reconnectBackoffBaseMs = 0;

        assertThatThrownBy(() -> new CoinbaseL2RecoveryStrategy(new RecordingActions(), config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reconnectBackoffBaseMs");
    }

    private static RecoveryRequest newRecoveryRequest(RecoveryRequestType requestType) {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L2,
                1001,
                requestType,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                123L);
    }

    private static TransportConfig validTransportConfig() {
        TransportConfig config = new TransportConfig();
        config.reconnectBackoffBaseMs = 10;
        config.reconnectBackoffMaxMs = 40;
        config.reconnectBackoffJitterMs = 5;
        return config;
    }

    private static final class Fixture {
        private final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        private final AtomicInteger restoredCount = new AtomicInteger();
        private final AtomicReference<String> failureReason = new AtomicReference<>();
        private final RecordingActions actions = new RecordingActions();
        private final List<Long> sleeps = new ArrayList<>();
        private final DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        private final CoinbaseL2RecoveryStrategy strategy = new CoinbaseL2RecoveryStrategy(
                actions,
                validTransportConfig(),
                3,
                () -> 5L,
                sleeps::add);
    }

    private static final class RecordingActions implements CoinbaseL2RecoveryStrategy.Actions {
        private final List<String> steps = new ArrayList<>();
        private boolean throwOnUnsubscribe;
        private int reconnectFailuresRemaining;

        @Override
        public void unsubscribe() {
            steps.add("unsubscribe");
            if (throwOnUnsubscribe) {
                throw new IllegalStateException("unsubscribe failed");
            }
        }

        @Override
        public void close() {
            steps.add("close");
        }

        @Override
        public void reconnect() {
            steps.add("reconnect");
            if (reconnectFailuresRemaining > 0) {
                reconnectFailuresRemaining--;
                throw new IllegalStateException("reconnect failed");
            }
        }

        @Override
        public void resubscribe() {
            steps.add("resubscribe");
        }
    }
}
