package io.rueishi.marketdata.crypto.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultRecoveryContext}.
 *
 * <p>These tests verify that each recovery context represents a single attempt:
 * it exposes per-instrument counters, invokes exactly one terminal callback, and
 * distinct attempts are represented by distinct context objects.</p>
 */
class DefaultRecoveryContextTest {

    /**
     * Verifies that recovery success invokes the channel-restored callback exactly once.
     */
    @Test
    void channelRestoredCallbackIsOneShot() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);

        context.onChannelRestored();

        assertThat(restoredCount).hasValue(1);
        assertThat(failureReason).hasValue(null);
        assertThat(context.counters()).isSameAs(counters);
        assertThatThrownBy(() -> context.onRecoveryFailed("late failure"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already signaled");
    }

    /**
     * Verifies that recovery failure invokes the failure callback exactly once.
     */
    @Test
    void failureCallbackIsOneShot() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicInteger restoredCount = new AtomicInteger();
        AtomicReference<String> failureReason = new AtomicReference<>();
        DefaultRecoveryContext context = new DefaultRecoveryContext(
                restoredCount::incrementAndGet,
                failureReason::set,
                counters);

        context.onRecoveryFailed("network timeout");

        assertThat(restoredCount).hasValue(0);
        assertThat(failureReason).hasValue("network timeout");
        assertThatThrownBy(context::onChannelRestored)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already signaled");
    }

    /**
     * Verifies that repeated recovery attempts use distinct context wrappers.
     */
    @Test
    void recoveryContextsAreDistinctPerAttempt() {
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        DefaultRecoveryContext first = new DefaultRecoveryContext(() -> { }, ignored -> { }, counters);
        DefaultRecoveryContext second = new DefaultRecoveryContext(() -> { }, ignored -> { }, counters);

        assertThat(first).isNotSameAs(second);
        assertThat(first.counters()).isSameAs(second.counters());
    }
}
