package io.rueishi.marketdata.crypto.core.connector;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Focused duplicate-recovery tests for {@link AbstractConnector}.
 *
 * <p>This class isolates the coalescing rule from the broader recovery state
 * test. It uses the connector's scheduled {@link Connector#requestRecovery(RecoveryRequest)}
 * entry point to prove downstream requests are executed on the connector
 * event-loop path while {@link AbstractConnector#recover(RecoveryRequest)}
 * remains the final guard for duplicate and mismatched requests.</p>
 */
class RecoveryCoalescingTest {

    /**
     * Verifies duplicate recovery requests are counted and ignored while recovery is active.
     */
    @Test
    void duplicateRecoveryRequestsAreCoalescedUntilSnapshotBoundaryCompletes() {
        AbstractConnectorLifecycleTest.TestHarness harness = new AbstractConnectorLifecycleTest.TestHarness();
        try {
            TestConnector connector = harness.newConnector();
            connector.init(harness.newContext());

            connector.requestRecovery(newRecoveryRequest());
            connector.requestRecovery(newRecoveryRequest());
            connector.requestRecovery(newRecoveryRequest());
            awaitUntil(() -> harness.recovery.executeCount.get() == 1
                    && connector.countersView().recoveryRequestsIgnoredInProgress().get() == 2);

            assertThat(harness.publisher.resetCount).hasValue(1);
            assertThat(harness.recovery.executeCount).hasValue(1);
            assertThat(connector.countersView().recoveryRequestsIgnoredInProgress().get()).isEqualTo(2);

            connector.snapshotContextView().snapshotGatekeeper().accept();
            connector.snapshotContextView().onSnapshotBoundaryAccepted();
            connector.requestRecovery(newRecoveryRequest());
            awaitUntil(() -> harness.recovery.executeCount.get() == 2);

            assertThat(harness.publisher.resetCount).hasValue(2);
            assertThat(harness.recovery.executeCount).hasValue(2);
            assertThat(connector.countersView().recoveryRequestsIgnoredInProgress().get()).isEqualTo(2);
        } finally {
            harness.close();
        }
    }

    /**
     * Verifies the base connector ignores request metadata that does not match
     * its venue or instrument, keeping the final guard in place even if a caller
     * bypasses the router.
     */
    @Test
    void mismatchedRecoveryRequestsAreIgnoredByConnectorGuard() {
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
                    io.rueishi.marketdata.crypto.core.config.VenueEnum.COINBASE_L2,
                    9999,
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.MANUAL_RESET,
                    123L));

            assertThat(harness.publisher.resetCount).hasValue(0);
            assertThat(harness.recovery.executeCount).hasValue(0);
        } finally {
            harness.close();
        }
    }

    private static RecoveryRequest newRecoveryRequest() {
        return new RecoveryRequest(
                io.rueishi.marketdata.crypto.core.config.VenueEnum.COINBASE_L2,
                1001,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123L);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for scheduled recovery", ex);
            }
        }
        throw new AssertionError("Timed out waiting for scheduled recovery");
    }
}
