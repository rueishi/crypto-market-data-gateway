package io.rueishi.marketdata.crypto.core.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotGatekeeper;
import org.junit.jupiter.api.Test;

/**
 * Recovery state-machine tests for {@link AbstractConnector}.
 *
 * <p>These tests drive recovery with a controllable strategy and subscribe-
 * driven snapshot strategy. They verify coalescing, reset publication, parse
 * context reuse, context replacement after Phase A, snapshot triggering, and
 * the Phase B completion point that clears recovery state and increments
 * counters.</p>
 */
class AbstractConnectorRecoveryStateTest {

    /**
     * Verifies that recovery stays in progress after Phase A and completes only after snapshot acceptance.
     */
    @Test
    void recoveryFlagSpansPhaseAAndPhaseB() {
        AbstractConnectorLifecycleTest.TestHarness harness = new AbstractConnectorLifecycleTest.TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());
            Object parseContext = connector.parseContextView();
            Object firstSnapshotContext = connector.snapshotContextView();
            Object firstRecoveryContext = connector.recoveryContextView();
            SnapshotGatekeeper firstGatekeeper = connector.parseContextView().snapshotGatekeeper();

            connector.recover(newRecoveryRequest());

            assertThat(connector.recoveryInProgressView()).isTrue();
            assertThat(connector.parseContextView()).isSameAs(parseContext);
            assertThat(connector.snapshotContextView()).isNotSameAs(firstSnapshotContext);
            assertThat(connector.recoveryContextView()).isNotSameAs(firstRecoveryContext);
            assertThat(connector.parseContextView().snapshotGatekeeper()).isNotSameAs(firstGatekeeper);
            assertThat(connector.snapshotContextView().snapshotGatekeeper())
                    .isSameAs(connector.parseContextView().snapshotGatekeeper());
            assertThat(connector.subscribePayloads).isEmpty();
            assertThat(harness.snapshot.triggerCount).hasValue(1);
            assertThat(harness.publisher.resetCount).hasValue(1);
            assertThat(connector.countersView().recoveryCompletions().get()).isZero();

            connector.recover(newRecoveryRequest());

            assertThat(harness.publisher.resetCount).hasValue(1);
            assertThat(connector.countersView().recoveryRequestsIgnoredInProgress().get()).isEqualTo(1);
            assertThat(connector.countersView().recoveryCompletions().get()).isZero();

            connector.snapshotContextView().snapshotGatekeeper().accept();
            connector.snapshotContextView().onSnapshotBoundaryAccepted();

            assertThat(connector.recoveryInProgressView()).isFalse();
            assertThat(connector.countersView().recoveryCompletions().get()).isEqualTo(1);
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies that a throwing recovery strategy does not leave recovery stuck in progress.
     */
    @Test
    void throwingRecoveryStrategyClearsRecoveryInProgress() {
        AbstractConnectorLifecycleTest.TestHarness harness = new AbstractConnectorLifecycleTest.TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());
            harness.recovery.throwFromExecute = true;

            connector.recover(newRecoveryRequest());

            assertThat(connector.recoveryInProgressView()).isFalse();
            assertThat(connector.countersView().recoveryAttempts().get()).isEqualTo(1);
            assertThat(connector.countersView().recoveryFailures().get()).isEqualTo(1);
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies that requests for another venue or instrument do not start recovery on this connector.
     */
    @Test
    void ignoresRecoveryRequestsForOtherConnectors() {
        AbstractConnectorLifecycleTest.TestHarness harness = new AbstractConnectorLifecycleTest.TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());

            connector.recover(new RecoveryRequest(
                    io.rueishi.marketdata.crypto.core.config.VenueEnum.BINANCE_L2,
                    1001,
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.MANUAL_RESET,
                    123L));
            connector.recover(new RecoveryRequest(
                    connector.venue(),
                    2002,
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.MANUAL_RESET,
                    123L));

            assertThat(harness.publisher.resetCount).hasValue(0);
            assertThat(connector.countersView().recoveryAttempts().get()).isZero();
            assertThat(connector.recoveryInProgressView()).isFalse();
        } finally {
            harness.close();
        }
    }

    private static RecoveryRequest newRecoveryRequest() {
        return new RecoveryRequest(
                io.rueishi.marketdata.crypto.core.config.VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                123L);
    }
}
