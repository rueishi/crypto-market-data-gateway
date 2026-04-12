package io.rueishi.marketdata.crypto.core.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Tests parser-context publisher retry and backpressure recovery behavior.
 *
 * <p>The suite uses {@link DefaultParseContext} with a scripted publisher so it
 * can exercise the Phase 3 handoff policy without a venue parser or live
 * connector. Encoder, counters, and recovery callbacks are real local
 * collaborators; only downstream publish acceptance is stubbed.</p>
 */
class PublisherBackpressureRecoveryTest {

    /**
     * Verifies transient backpressure is retried and does not request recovery.
     */
    @Test
    void transientBackpressureSucceedsOnRetryWithoutRecovery() {
        ScriptedPublisher publisher = new ScriptedPublisher(false, true);
        Fixture fixture = new Fixture(publisher);

        fixture.beginOneLevelMessage(100L, 150L);

        assertThat(fixture.context.publishEncodedMessage("transient backpressure")).isTrue();
        assertThat(publisher.attempts).isEqualTo(2);
        assertThat(fixture.recoverySignals).isEmpty();
        assertThat(fixture.counters.backpressureEvents().get()).isZero();
        assertThat(fixture.counters.backpressureDrops().get()).isZero();
        assertThat(fixture.counters.handoffLatencyLastNanos().get()).isEqualTo(50L);
        assertThat(fixture.counters.handoffLatencyMinNanos().get()).isEqualTo(50L);
        assertThat(fixture.counters.handoffLatencyMaxNanos().get()).isEqualTo(50L);
    }

    /**
     * Verifies three rejected publish attempts trigger reset recovery and backpressure counters once.
     */
    @Test
    void exhaustedBackpressureTriggersRecoveryAfterThreeAttempts() {
        ScriptedPublisher publisher = new ScriptedPublisher(false, false, false);
        Fixture fixture = new Fixture(publisher);

        fixture.beginOneLevelMessage(100L, 150L);

        assertThat(fixture.context.publishEncodedMessage("publisher full")).isFalse();
        assertThat(publisher.attempts).isEqualTo(3);
        assertThat(fixture.counters.backpressureEvents().get()).isEqualTo(1);
        assertThat(fixture.counters.backpressureDrops().get()).isEqualTo(1);
        assertThat(fixture.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                    assertThat(signal.diagnosticText()).isEqualTo("publisher full");
                });
    }

    /**
     * Verifies publisher exceptions increment publish-failure counters and remain visible to callers.
     */
    @Test
    void publisherExceptionIncrementsPublishFailuresAndPropagates() {
        ScriptedPublisher publisher = new ScriptedPublisher(true);
        publisher.throwOnAttempt = 1;
        Fixture fixture = new Fixture(publisher);

        fixture.beginOneLevelMessage(100L, 150L);

        assertThatThrownBy(() -> fixture.context.publishEncodedMessage("throwing publisher"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publisher failed");
        assertThat(fixture.counters.publishFailures().get()).isEqualTo(1);
        assertThat(fixture.recoverySignals).isEmpty();
    }

    private static final class Fixture {
        private final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        private final AtomicLong nowNanos = new AtomicLong();
        private final List<RecoverySignal> recoverySignals = new ArrayList<>();
        private final DefaultParseContext context;

        private Fixture(Publisher publisher) {
            this.context = new DefaultParseContext(
                    CoreTestFixtures.newEncoder(),
                    publisher,
                    counters,
                    nowNanos::get,
                    (type, reason, diagnosticText) -> recoverySignals.add(
                            new RecoverySignal(type, reason, diagnosticText)),
                    () -> {
                    },
                    () -> {
                    });
        }

        private void beginOneLevelMessage(long ingressNanos, long handoffNanos) {
            nowNanos.set(ingressNanos);
            context.encoder().beginMessage(
                    EncodingConstants.EVENT_TYPE_BOOK_UPDATE,
                    1L,
                    1L,
                    1L,
                    -1L,
                    ingressNanos);
            context.encoder().writeLevel((byte) 1, (byte) 1, 100L, (byte) 1, 200L, (byte) 2);
            nowNanos.set(handoffNanos);
        }
    }

    private static final class ScriptedPublisher implements Publisher {
        private final boolean[] results;
        private int attempts;
        private int throwOnAttempt;

        private ScriptedPublisher(boolean... results) {
            this.results = results;
        }

        @Override
        public boolean publish(
                DirectBuffer buffer,
                int offset,
                int length,
                InstrumentCounters counters,
                NanoClock nanoClock) {
            attempts++;
            if (attempts == throwOnAttempt) {
                throw new IllegalStateException("publisher failed");
            }
            return results[Math.min(attempts - 1, results.length - 1)];
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

    private record RecoverySignal(
            RecoveryRequestType type,
            RecoveryReasonCode reason,
            String diagnosticText) {
    }
}
