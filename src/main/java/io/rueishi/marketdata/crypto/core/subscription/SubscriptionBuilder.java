package io.rueishi.marketdata.crypto.core.subscription;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;

/**
 * Builds venue subscription control-plane payloads for a configured instrument.
 *
 * <p>Connector startup and shutdown code calls this interface when it needs the
 * exchange-specific subscribe or unsubscribe JSON for an instrument. Concrete
 * venue builders own authentication, channel names, and exact JSON shape; this
 * shared interface fixes the compile-time contract without implementing any
 * venue-specific behavior in {@code core}. Phase 2 Coinbase builders use one
 * UTF-8 JSON text-frame payload per method call.</p>
 */
public interface SubscriptionBuilder {

    /**
     * Builds a subscribe payload for the supplied instrument.
     *
     * <p>The returned bytes must contain UTF-8 encoded JSON ready to write to
     * the WebSocket text-frame path. Implementations may use credential and
     * venue configuration captured at construction time, but should not mutate
     * the supplied instrument object.</p>
     *
     * @param instrument configured instrument to subscribe to
     * @return UTF-8 JSON subscribe payload bytes
     */
    byte[] buildSubscribe(InstrumentConfig instrument);

    /**
     * Builds an unsubscribe payload for the supplied instrument.
     *
     * <p>The returned bytes must contain UTF-8 encoded JSON ready to write to
     * the WebSocket text-frame path. Implementations may use credential and
     * venue configuration captured at construction time, but should not mutate
     * the supplied instrument object.</p>
     *
     * @param instrument configured instrument to unsubscribe from
     * @return UTF-8 JSON unsubscribe payload bytes
     */
    byte[] buildUnsubscribe(InstrumentConfig instrument);
}
