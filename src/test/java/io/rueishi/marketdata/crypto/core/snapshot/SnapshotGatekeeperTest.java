package io.rueishi.marketdata.crypto.core.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SnapshotGatekeeper}.
 *
 * <p>These tests cover the session boundary that blocks incremental
 * {@code BOOK_UPDATE} publication until a valid snapshot is accepted. No
 * dependencies are mocked because the gate is a standalone state holder used by
 * later parser and context code.</p>
 */
class SnapshotGatekeeperTest {

    /**
     * Verifies that a new gate blocks updates before any snapshot boundary is accepted.
     */
    @Test
    void startsClosedBeforeSnapshotAcceptance() {
        SnapshotGatekeeper gatekeeper = new SnapshotGatekeeper();

        assertThat(gatekeeper.isReady()).isFalse();
    }

    /**
     * Verifies that accepting a valid snapshot boundary opens the update gate.
     */
    @Test
    void acceptOpensGateForIncrementalUpdates() {
        SnapshotGatekeeper gatekeeper = new SnapshotGatekeeper();

        gatekeeper.accept();

        assertThat(gatekeeper.isReady()).isTrue();
    }

    /**
     * Verifies that repeated accept calls are idempotent and keep the gate open.
     */
    @Test
    void repeatedAcceptKeepsGateReady() {
        SnapshotGatekeeper gatekeeper = new SnapshotGatekeeper();

        gatekeeper.accept();
        gatekeeper.accept();

        assertThat(gatekeeper.isReady()).isTrue();
    }

    /**
     * Verifies that recovery/session reset closes the gate until a fresh snapshot is accepted.
     */
    @Test
    void resetReturnsGateToWaitingState() {
        SnapshotGatekeeper gatekeeper = new SnapshotGatekeeper();

        gatekeeper.accept();
        gatekeeper.reset();

        assertThat(gatekeeper.isReady()).isFalse();
    }

    /**
     * Verifies multiple reset and accept cycles stay deterministic across sessions.
     */
    @Test
    void multipleSessionsRequireSnapshotAcceptanceEachTime() {
        SnapshotGatekeeper gatekeeper = new SnapshotGatekeeper();

        gatekeeper.accept();
        assertThat(gatekeeper.isReady()).isTrue();
        gatekeeper.reset();
        assertThat(gatekeeper.isReady()).isFalse();
        gatekeeper.accept();
        assertThat(gatekeeper.isReady()).isTrue();
    }
}
