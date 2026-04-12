package io.rueishi.marketdata.crypto.core.transport;

import io.netty.channel.EventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.CpuAffinityConfig;
import io.rueishi.marketdata.crypto.core.config.CpuAffinityMode;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Optional CPU affinity bridge for connector event-loop threads.
 *
 * <p>{@code CpuAffinitySupport} is called during bootstrap after a connector's
 * Netty {@link EventLoopGroup} has been created. It runs the affinity
 * acquisition on the event-loop thread itself, because CPU pinning applies to
 * the current Java thread. The implementation uses reflection against OpenHFT
 * {@code net.openhft.affinity.AffinityLock} so deployments that do not include
 * the optional library still start when affinity is disabled or best-effort.</p>
 *
 * <p>When {@code busyWaitEnabled} is true, affinity is mandatory and startup
 * fails if the provider is unavailable or cannot acquire a lock. Otherwise
 * unsupported platforms and missing libraries degrade to a no-op closeable.</p>
 */
public final class CpuAffinitySupport {
    private static final AutoCloseable NOOP_LOCK = () -> {
    };

    private CpuAffinitySupport() {
    }

    /**
     * Acquires an optional CPU affinity lock for one connector event-loop group.
     *
     * <p>The returned closeable must be closed by the gateway runtime after the
     * event-loop group has stopped accepting work. In explicit mode, {@code
     * connectorIndex} selects the CPU id from {@link CpuAffinityConfig#eventLoopCpuIds}
     * using connector/instrument creation order.</p>
     *
     * @param eventLoopGroup connector-owned event-loop group
     * @param config CPU affinity configuration
     * @param busyWaitEnabled whether affinity is mandatory for busy-wait operation
     * @param connectorIndex connector index in instrument order
     * @return closeable affinity lock, or a no-op when affinity is disabled or unavailable by policy
     * @throws NullPointerException if required arguments are null
     * @throws IllegalStateException if affinity is mandatory or explicitly misconfigured and cannot be acquired
     */
    public static AutoCloseable acquire(
            EventLoopGroup eventLoopGroup,
            CpuAffinityConfig config,
            boolean busyWaitEnabled,
            int connectorIndex) {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        Objects.requireNonNull(config, "config");
        if (!config.enabled) {
            return NOOP_LOCK;
        }
        try {
            return eventLoopGroup.next().submit((Callable<AutoCloseable>) () ->
                    acquireOnCurrentThread(config, connectorIndex)).syncUninterruptibly().getNow();
        } catch (RuntimeException ex) {
            if (busyWaitEnabled) {
                throw ex;
            }
            return NOOP_LOCK;
        }
    }

    private static AutoCloseable acquireOnCurrentThread(CpuAffinityConfig config, int connectorIndex) {
        try {
            Class<?> affinityLockClass = Class.forName("net.openhft.affinity.AffinityLock");
            Object lock;
            if (config.mode == CpuAffinityMode.EXPLICIT) {
                if (connectorIndex >= config.eventLoopCpuIds.size()) {
                    throw new IllegalStateException("Not enough explicit CPU ids for connector index " + connectorIndex);
                }
                Method acquireLock = affinityLockClass.getMethod("acquireLock", int.class);
                lock = acquireLock.invoke(null, config.eventLoopCpuIds.get(connectorIndex));
            } else {
                Method acquireLock = affinityLockClass.getMethod("acquireLock");
                lock = acquireLock.invoke(null);
            }
            return closeableLock(lock);
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("OpenHFT affinity library is not available", ex);
        } catch (NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalStateException("OpenHFT affinity API is not compatible", ex);
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause() == null ? ex : ex.getCause();
            throw new IllegalStateException("Unable to acquire CPU affinity lock", cause);
        }
    }

    private static AutoCloseable closeableLock(Object lock) {
        Objects.requireNonNull(lock, "lock");
        return () -> {
            try {
                if (lock instanceof AutoCloseable closeable) {
                    closeable.close();
                    return;
                }
                Method release = lock.getClass().getMethod("release");
                release.invoke(lock);
            } catch (NoSuchMethodException ex) {
                throw new IllegalStateException("OpenHFT affinity lock has no close or release method", ex);
            }
        };
    }
}
