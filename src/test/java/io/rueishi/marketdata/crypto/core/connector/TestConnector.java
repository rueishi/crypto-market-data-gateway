package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.recovery.DefaultRecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.snapshot.DefaultSnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only concrete connector used by {@link AbstractConnector} tests.
 *
 * <p>The class exposes protected base-state accessors and records subscribe and
 * unsubscribe hook payloads. It does not add production behavior; it exists so
 * the tests can verify the abstract base class without creating a real venue
 * package or live transport.</p>
 */
final class TestConnector extends AbstractConnector {
    final List<String> subscribePayloads = new ArrayList<>();
    final List<String> unsubscribePayloads = new ArrayList<>();

    TestConnector(
            InstrumentConfig instrument,
            FeedParser parser,
            SnapshotStrategy snapshot,
            RecoveryStrategy recovery,
            SubscriptionBuilder subscription) {
        super(instrument, parser, snapshot, recovery, subscription);
    }

    @Override
    protected VenueEnum venueEnum() {
        return VenueEnum.COINBASE_L2;
    }

    @Override
    protected int maxEntryCount() {
        return 4;
    }

    @Override
    protected int headroomBytes() {
        return 0;
    }

    @Override
    protected void onSubscribe(byte[] payload) {
        subscribePayloads.add(new String(payload, StandardCharsets.UTF_8));
    }

    @Override
    protected void onUnsubscribe(byte[] payload) {
        unsubscribePayloads.add(new String(payload, StandardCharsets.UTF_8));
    }

    DefaultParseContext parseContextView() {
        return parseContext();
    }

    DefaultSnapshotContext snapshotContextView() {
        return snapshotContext();
    }

    DefaultRecoveryContext recoveryContextView() {
        return recoveryContext();
    }

    boolean recoveryInProgressView() {
        return recoveryInProgress();
    }

    InstrumentCounters countersView() {
        return counters();
    }
}
