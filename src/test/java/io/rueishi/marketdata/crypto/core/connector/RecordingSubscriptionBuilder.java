package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import java.nio.charset.StandardCharsets;

/**
 * Test subscription builder that emits deterministic text payloads.
 */
final class RecordingSubscriptionBuilder implements SubscriptionBuilder {

    @Override
    public byte[] buildSubscribe(InstrumentConfig instrument) {
        return ("subscribe:" + instrument.exchangeSymbol).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] buildUnsubscribe(InstrumentConfig instrument) {
        return ("unsubscribe:" + instrument.exchangeSymbol).getBytes(StandardCharsets.UTF_8);
    }
}
