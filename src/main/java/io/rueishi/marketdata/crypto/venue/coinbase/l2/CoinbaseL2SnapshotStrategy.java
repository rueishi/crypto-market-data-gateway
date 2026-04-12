package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.rueishi.marketdata.crypto.core.snapshot.SubscribeDrivenSnapshotStrategy;

/**
 * Coinbase L2 snapshot strategy for subscribe-driven snapshot acquisition.
 *
 * <p>Coinbase sends the initial L2 book snapshot on the WebSocket {@code level2}
 * channel after subscription, so this strategy only identifies the venue mode
 * to connector lifecycle code. {@link CoinbaseL2FeedParser} owns the runtime
 * snapshot boundary: it publishes the snapshot, opens the parser gate, and
 * signals boundary completion through the parser context. Connector factories
 * typically instantiate this class once with the Coinbase L2 parser, recovery
 * strategy, and subscription builder.</p>
 *
 * @see SubscribeDrivenSnapshotStrategy
 */
public final class CoinbaseL2SnapshotStrategy extends SubscribeDrivenSnapshotStrategy {
}
