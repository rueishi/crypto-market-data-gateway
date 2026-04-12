package io.rueishi.marketdata.crypto.core.config;

/**
 * I/O transport selection policy for connector event-loop groups.
 *
 * <p>Controls whether the gateway prefers {@code io_uring}, forces {@code epoll},
 * or delegates the decision to runtime availability detection. The selected
 * transport applies to every connector in the process — one process uses one
 * transport type throughout.</p>
 */
public enum IoTransport {

    /**
     * Attempt {@code io_uring} first. If the kernel does not support it (kernel
     * earlier than 5.9, or the incubator native library is absent from the
     * classpath), fall back to {@code epoll} automatically. If {@code epoll} is
     * also unavailable (non-Linux environment such as a developer workstation or
     * CI runner), fall back to NIO. Logs which transport was selected at startup.
     *
     * <p>Recommended for production — gets {@code io_uring} where available
     * without requiring operator intervention when deploying across mixed kernel
     * versions.</p>
     */
    AUTO,

    /**
     * Force Linux {@code epoll}. Never attempts {@code io_uring} regardless of
     * kernel version. Fails startup with {@link IllegalStateException} if epoll
     * is unavailable (non-Linux environment).
     *
     * <p>Use when {@code io_uring} is known to be problematic on the target
     * kernel, or when deterministic transport selection is required.</p>
     */
    EPOLL,

    /**
     * Require {@code io_uring}. Fails startup with {@link IllegalStateException}
     * if {@code io_uring} is unavailable (kernel earlier than 5.9 or missing
     * native library). Use in production environments where {@code io_uring} has
     * been validated and silent fallback to {@code epoll} is unacceptable.
     */
    IO_URING
}
