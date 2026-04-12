package io.rueishi.marketdata.crypto.core.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SequenceTracker}.
 *
 * <p>These tests cover deterministic connector-session sequencing: first
 * sequence after construction, non-mutating reads, monotonic increments, and
 * reset behavior across recovery/session boundaries. No dependencies are
 * mocked because the tracker is an isolated mechanical state holder.</p>
 */
class SequenceTrackerTest {

    /**
     * Verifies that a new tracker starts at zero and the first consumed sequence is one.
     */
    @Test
    void startsAtOneAfterConstruction() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.current()).isZero();
        assertThat(tracker.next()).isEqualTo(1);
        assertThat(tracker.current()).isEqualTo(1);
    }

    /**
     * Verifies that reading the current value does not consume a sequence number.
     */
    @Test
    void currentDoesNotIncrementSequence() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.next()).isEqualTo(1);
        assertThat(tracker.current()).isEqualTo(1);
        assertThat(tracker.current()).isEqualTo(1);
        assertThat(tracker.next()).isEqualTo(2);
    }

    /**
     * Verifies that recovery/session reset returns the tracker to the zero state.
     */
    @Test
    void resetReturnsTrackerToZeroState() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.next()).isEqualTo(1);
        assertThat(tracker.next()).isEqualTo(2);

        tracker.reset();

        assertThat(tracker.current()).isZero();
        assertThat(tracker.next()).isEqualTo(1);
    }

    /**
     * Verifies multiple reset cycles stay deterministic across repeated sessions.
     */
    @Test
    void multipleResetsRestartSequencesAtOne() {
        SequenceTracker tracker = new SequenceTracker();

        tracker.next();
        tracker.reset();
        assertThat(tracker.next()).isEqualTo(1);

        tracker.next();
        tracker.next();
        tracker.reset();
        assertThat(tracker.next()).isEqualTo(1);
    }
}
