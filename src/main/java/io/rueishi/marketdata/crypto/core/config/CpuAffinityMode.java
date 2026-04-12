package io.rueishi.marketdata.crypto.core.config;

/**
 * CPU affinity acquisition mode for connector event-loop threads.
 *
 * <p>{@code CpuAffinityMode} is read from {@link CpuAffinityConfig} during
 * bootstrap. It is not used on the parser hot path. {@link #AUTO} lets the
 * affinity provider choose a suitable core, while {@link #EXPLICIT} maps
 * connector event-loop threads to configured CPU ids in instrument order.</p>
 */
public enum CpuAffinityMode {
    /** Let the affinity provider choose an available CPU for each event-loop thread. */
    AUTO,
    /** Pin each event-loop thread to the corresponding configured CPU id. */
    EXPLICIT
}
