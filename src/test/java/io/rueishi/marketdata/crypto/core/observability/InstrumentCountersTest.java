package io.rueishi.marketdata.crypto.core.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InstrumentCounters}.
 *
 * <p>These tests verify the per-instrument counter bundle returned by
 * {@link GatewayCounters#forInstrument(int)} during connector initialization.
 * The suite protects the stable accessor surface that parsers, encoders,
 * publishers, recovery code, and liveness checks will use later. It also checks
 * that accessors return pre-allocated Agrona counters and that the Phase 1 clock
 * decision remains documented as direct Agrona {@link NanoClock}/{@link EpochClock}
 * usage rather than local wrapper interfaces.</p>
 */
class InstrumentCountersTest {

    /**
     * Verifies that the public accessor API exposes exactly the Phase 1
     * instrument counters expected by later parser, encoder, publisher,
     * recovery, and liveness task cards.
     */
    @Test
    void exposesStablePhaseOneCounterAccessorSurface() {
        Set<String> methods = Arrays.stream(InstrumentCounters.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> method.getParameterCount() == 0)
                .filter(method -> method.getReturnType().equals(AtomicCounter.class))
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertThat(methods).containsExactlyInAnyOrder(
                "framesReceived",
                "bytesReceived",
                "messagesDecoded",
                "snapshotMessagesReceived",
                "updateMessagesReceived",
                "heartbeatsReceived",
                "parseFailures",
                "malformedRejections",
                "unknownTypeDrops",
                "preSnapshotDrops",
                "unknownSymbolDrops",
                "productIdMismatches",
                "encodeSuccesses",
                "encodeFailures",
                "overflowRejections",
                "levelsEncoded",
                "encodedBytes",
                "encodeBufferReuseCount",
                "messagesPublished",
                "publishFailures",
                "backpressureEvents",
                "backpressureDrops",
                "recoveryAttempts",
                "recoveryCompletions",
                "recoveryFailures",
                "recoveryExecutions",
                "recoveryRequestsIgnoredInProgress",
                "bookResetPublished",
                "bookSnapshotPublished",
                "subscriptionValidationFailures",
                "sequenceGapsDetected",
                "outOfOrderMessages",
                "checksumFailures",
                "heartbeatsMissed",
                "livenessFailures",
                "handoffLatencyMinNanos",
                "handoffLatencyMaxNanos",
                "handoffLatencyLastNanos",
                "publisherLatencyMinNanos",
                "publisherLatencyMaxNanos",
                "publisherLatencyLastNanos",
                "lastHeartbeatReceivedNanos",
                "lastMessageReceivedNanos");
    }

    /**
     * Verifies that accessor calls return stable constructor-allocated counters
     * and that those counters support normal Agrona mutation operations.
     */
    @Test
    void accessorsReturnPreAllocatedStableCounters() {
        try (GatewayCounters gatewayCounters =
                     new GatewayCounters(GatewayCountersTest.newCountersManager(), "gateway-1", "test", "STUB")) {
            InstrumentCounters counters = gatewayCounters.forInstrument(1001);

            assertThat(counters.unknownTypeDrops()).isSameAs(counters.unknownTypeDrops());
            assertThat(counters.messagesPublished()).isSameAs(counters.messagesPublished());
            assertThat(counters.encodeBufferReuseCount()).isSameAs(counters.encodeBufferReuseCount());

            counters.unknownTypeDrops().increment();
            counters.messagesPublished().getAndAdd(7);
            counters.lastHeartbeatReceivedNanos().set(123_456L);

            assertThat(counters.unknownTypeDrops().get()).isEqualTo(1);
            assertThat(counters.messagesPublished().get()).isEqualTo(7);
            assertThat(counters.lastHeartbeatReceivedNanos().get()).isEqualTo(123_456L);
        }
    }

    /**
     * Verifies that an instrument-scoped counter label includes both gateway
     * dimensions and the instrument id dimension.
     */
    @Test
    void counterLabelsIncludeInstrumentDimension() {
        try (GatewayCounters gatewayCounters =
                     new GatewayCounters(GatewayCountersTest.newCountersManager(), "gateway-1", "test", "STUB")) {
            InstrumentCounters counters = gatewayCounters.forInstrument(1001);

            assertThat(counters.productIdMismatches().label())
                    .contains("gateway_product_id_mismatches")
                    .contains("instanceId=gateway-1")
                    .contains("environment=test")
                    .contains("venue=STUB")
                    .contains("instrumentId=1001");
        }
    }

    /**
     * Verifies the Phase 1 decision to use Agrona clock interfaces directly and
     * keep local clock wrapper interfaces out of the observability package.
     *
     * @throws Exception if the package-info source file cannot be read
     */
    @Test
    void phaseOneClockDecisionUsesAgronaInterfacesDirectly() throws Exception {
        assertThat(NanoClock.class.getName()).isEqualTo("org.agrona.concurrent.NanoClock");
        assertThat(EpochClock.class.getName()).isEqualTo("org.agrona.concurrent.EpochClock");

        String packageInfo = Files.readString(Path.of(
                "src/main/java/io/rueishi/marketdata/crypto/core/observability/package-info.java"));
        assertThat(packageInfo)
                .contains("NanoClock")
                .contains("EpochClock")
                .contains("does not introduce");
    }
}
