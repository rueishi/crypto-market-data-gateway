package io.rueishi.marketdata.crypto.core.config;

import java.util.List;
import java.util.Map;

/**
 * Top-level startup configuration for one gateway process.
 *
 * <p>{@code GatewayConfig} is populated by {@link ConfigLoader} from the flat
 * YAML schema. Bootstrap consumes the validated object once at startup to
 * create shared resources, discover the connector factory for {@link #venue},
 * and create one connector per entry in {@link #instruments}. Public field
 * names intentionally match YAML keys so startup parsing stays simple and
 * off the hot path.</p>
 */
public final class GatewayConfig {
    /** Gateway process identity used in counters and operational metadata. */
    public String instanceId;
    /** Runtime environment label such as {@code prod}, {@code test}, or {@code dev}. */
    public String environment;
    /** Atomic exchange/depth identity for this single-venue process. */
    public VenueEnum venue;
    /** Instruments for which bootstrap creates connector instances. */
    public List<InstrumentConfig> instruments;
    /** Transport timing and frame-size settings. */
    public TransportConfig transport;
    /** Encoder capacity and headroom settings. */
    public EncodingConfig encoding;
    /** Counter, error-log, and metrics endpoint settings. */
    public ObservabilityConfig observability;
    /** Publisher backend selection and per-backend options. */
    public PublisherConfig publisher;
    /** Raw venue-specific YAML block extracted by {@link ConfigLoader}. */
    public Map<String, Object> venueConfig;
}
