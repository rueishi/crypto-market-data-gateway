package io.rueishi.marketdata.crypto.core.config;

/**
 * Instrument entry from the gateway startup configuration.
 *
 * <p>{@code InstrumentConfig} is populated once by {@link ConfigLoader} from
 * the {@code instruments} YAML list and then validated by
 * {@link ConfigValidator}. Bootstrap passes one instance per configured
 * instrument into connector factory code, and venue subscription builders read
 * {@link #exchangeSymbol} while encoders use {@link #instrumentId} for the
 * stable downstream identifier.</p>
 */
public final class InstrumentConfig {
    /**
     * Exchange-native symbol, such as {@code BTC-USD}, used by venue-specific subscription builders.
     */
    public String exchangeSymbol;

    /**
     * Stable internal instrument id encoded into downstream gateway output.
     */
    public int instrumentId;
}
