package io.rueishi.marketdata.crypto.core.config;

import java.util.List;

/**
 * Optional CPU affinity configuration for connector event-loop threads.
 *
 * <p>{@code CpuAffinityConfig} is nested under {@link TransportConfig} and is
 * consumed during bootstrap when connector-local Netty event-loop groups are
 * created. When disabled, bootstrap does not attempt to pin threads. In
 * {@link CpuAffinityMode#AUTO}, the affinity provider chooses a CPU; in
 * {@link CpuAffinityMode#EXPLICIT}, {@link #eventLoopCpuIds} maps configured
 * instruments to CPU ids in connector creation order.</p>
 */
public final class CpuAffinityConfig {
    /** Whether bootstrap should attempt event-loop CPU pinning. */
    public boolean enabled;
    /** Acquisition mode used when {@link #enabled} is true. */
    public CpuAffinityMode mode = CpuAffinityMode.AUTO;
    /** CPU ids used by {@link CpuAffinityMode#EXPLICIT} in instrument order. */
    public List<Integer> eventLoopCpuIds = List.of();
}
