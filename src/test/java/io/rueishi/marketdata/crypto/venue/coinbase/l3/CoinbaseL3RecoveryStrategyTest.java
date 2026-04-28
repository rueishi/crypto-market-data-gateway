package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.recovery.DefaultRecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CoinbaseL3RecoveryStrategy}.
 *
 * <p>The suite uses fake connector-owned
 * {@link CoinbaseL3RecoveryStrategy.Actions} plus a real
 * {@link DefaultRecoveryContext} with in-memory counters. It verifies the
 * Phase 4 Coinbase recovery sequence, success and failure terminal callbacks,
 * exponential reconnect backoff with deterministic jitter, shutdown/interrupt
 * behavior, and that the L3 resubscribe path emits the authenticated
 * {@code full}/{@code heartbeat} Coinbase payload built by
 * {@link CoinbaseL3SubscriptionBuilder}.</p>
 */
class CoinbaseL3RecoveryStrategyTest {

    /**
     * Verifies successful recovery executes unsubscribe, close, reconnect, and resubscribe in order.
     */
    @Test
    void execute_happyPath_callsAllFourStepsInOrder() {
        Fixture fixture = new Fixture();

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.actions.steps).containsExactly("unsubscribe", "close", "reconnect", "resubscribe");
    }

    /**
     * Verifies successful recovery signals channel restoration exactly once and never reports failure.
     */
    @Test
    void execute_onChannelRestored_calledExactlyOnce() {
        Fixture fixture = new Fixture();

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.restoredCount).hasValue(1);
        assertThat(fixture.failureReason).hasValue(null);
    }

    /**
     * Verifies reconnect failure exhausts the recovery attempt and reports a single failure reason.
     */
    @Test
    void execute_doReconnectFails_callsOnRecoveryFailed() {
        Fixture fixture = new Fixture();
        fixture.actions.reconnectFailuresRemaining = 3;

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.restoredCount).hasValue(0);
        assertThat(fixture.failureReason).hasValue("reconnect failed");
    }

    /**
     * Verifies resubscribe failure reports recovery failure after the reconnect succeeds.
     */
    @Test
    void execute_doResubscribeFails_callsOnRecoveryFailed() {
        Fixture fixture = new Fixture();
        fixture.actions.throwOnResubscribe = true;

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.actions.steps).containsExactly("unsubscribe", "close", "reconnect", "resubscribe");
        assertThat(fixture.restoredCount).hasValue(0);
        assertThat(fixture.failureReason).hasValue("resubscribe failed");
    }

    /**
     * Verifies the base reconnect template increments recovery execution counters on each attempt.
     */
    @Test
    void execute_incrementsRecoveryExecutionsCounter() {
        Fixture fixture = new Fixture();

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.counters.recoveryExecutions().get()).isEqualTo(1);
    }

    /**
     * Verifies the L3 resubscribe step uses the authenticated full plus heartbeat Coinbase payload.
     */
    @Test
    void execute_subscribesToFullAndHeartbeatChannels() {
        Fixture fixture = new Fixture();

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.actions.lastSubscribePayload)
                .contains("\"type\":\"subscribe\"")
                .contains("\"full\"")
                .contains("\"heartbeat\"");
    }

    /**
     * Verifies interrupted backoff sleep exits recovery and preserves the thread interrupt flag.
     */
    @Test
    void reconnectWithBackoff_shutdownRequested_exits() {
        Fixture fixture = new Fixture();
        fixture.actions.reconnectFailuresRemaining = 1;
        fixture.throwOnSleep = true;

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.failureReason).hasValue("sleep interrupted");
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted();
    }

    /**
     * Verifies reconnect retries apply exponential backoff plus deterministic jitter between attempts.
     */
    @Test
    void reconnectWithBackoff_exponentialBackoffApplied() {
        Fixture fixture = new Fixture();
        fixture.actions.reconnectFailuresRemaining = 2;

        fixture.strategy.execute(newRecoveryRequest(), fixture.context);

        assertThat(fixture.actions.steps).containsExactly(
                "unsubscribe", "close", "reconnect", "reconnect", "reconnect", "resubscribe");
        assertThat(fixture.sleeps).containsExactly(15L, 25L);
    }

    private static RecoveryRequest newRecoveryRequest() {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L3,
                1001,
                RecoveryRequestType.RESET,
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

    private static CoinbaseL3SubscriptionBuilder newSubscriptionBuilder() {
        CoinbaseConfig config = new CoinbaseConfig(
                URI.create("wss://ws-feed.exchange.coinbase.com"),
                "key-1",
                "c2VjcmV0",
                "passphrase-1",
                List.of("full", "heartbeat"));
        return new CoinbaseL3SubscriptionBuilder(config, new CoinbaseAuthenticator(config), () -> 1_700_000_000_999L);
    }

    private static final class Fixture {
        private final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        private final AtomicInteger restoredCount = new AtomicInteger();
        private final AtomicReference<String> failureReason = new AtomicReference<>();
        private final RecordingActions actions = new RecordingActions(newSubscriptionBuilder());
        private final List<Long> sleeps = new ArrayList<>();
        private boolean throwOnSleep;
        private final DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);
        private final CoinbaseL3RecoveryStrategy strategy = new CoinbaseL3RecoveryStrategy(
                actions,
                validTransportConfig(),
                3,
                () -> 5L,
                delayMs -> {
                    if (throwOnSleep) {
                        throw new InterruptedException("sleep interrupted");
                    }
                    sleeps.add(delayMs);
                });
    }

    private static final class RecordingActions implements CoinbaseL3RecoveryStrategy.Actions {
        private final CoinbaseL3SubscriptionBuilder subscriptionBuilder;
        private final List<String> steps = new ArrayList<>();
        private String lastSubscribePayload;
        private int reconnectFailuresRemaining;
        private boolean throwOnResubscribe;

        private RecordingActions(CoinbaseL3SubscriptionBuilder subscriptionBuilder) {
            this.subscriptionBuilder = subscriptionBuilder;
        }

        @Override
        public void unsubscribe() {
            steps.add("unsubscribe");
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
            if (throwOnResubscribe) {
                throw new IllegalStateException("resubscribe failed");
            }
            lastSubscribePayload = new String(
                    subscriptionBuilder.buildSubscribe(instrument("BTC-USD")),
                    StandardCharsets.UTF_8);
        }

        private static io.rueishi.marketdata.crypto.core.config.InstrumentConfig instrument(String exchangeSymbol) {
            io.rueishi.marketdata.crypto.core.config.InstrumentConfig instrument =
                    new io.rueishi.marketdata.crypto.core.config.InstrumentConfig();
            instrument.exchangeSymbol = exchangeSymbol;
            instrument.instrumentId = 1001;
            return instrument;
        }
    }
}
