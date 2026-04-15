package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.recovery.ReconnectRecoveryStrategy;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Coinbase L3 full reconnect recovery strategy.
 *
 * <p>{@code CoinbaseL3RecoveryStrategy} is the Phase 4 venue strategy invoked
 * by connector recovery flow after a recovery request has already published
 * {@code BOOK_RESET}. It extends the shared
 * {@link ReconnectRecoveryStrategy} template and delegates Coinbase transport
 * details through a narrow connector-owned {@link Actions} interface so the
 * strategy owns recovery ordering and reconnect backoff while the future
 * Coinbase L3 connector owns actual WebSocket channel operations and snapshot
 * triggering.</p>
 *
 * <p>All Coinbase L3 recovery request types use the same full path in this
 * phase: best-effort unsubscribe, close, reconnect with exponential backoff
 * and jitter, authenticated resubscribe to {@code full} plus
 * {@code heartbeat}, then {@link RecoveryContext#onChannelRestored()} exactly
 * once so connector lifecycle code can rebuild session state and trigger the
 * REST-then-delta snapshot strategy.</p>
 */
public final class CoinbaseL3RecoveryStrategy extends ReconnectRecoveryStrategy {
    private static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;

    private final Actions actions;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final long jitterBoundMs;
    private final int maxReconnectAttempts;
    private final LongSupplier jitterSupplier;
    private final Sleeper sleeper;

    /**
     * Creates a production Coinbase L3 recovery strategy from connector actions and transport config.
     *
     * @param actions connector-owned recovery actions
     * @param transportConfig startup transport configuration containing reconnect backoff settings
     * @throws NullPointerException if {@code actions} or {@code transportConfig} is null
     * @throws IllegalArgumentException if reconnect timing settings are invalid
     */
    public CoinbaseL3RecoveryStrategy(Actions actions, TransportConfig transportConfig) {
        this(
                actions,
                transportConfig,
                DEFAULT_MAX_RECONNECT_ATTEMPTS,
                () -> nextJitter(transportConfig.reconnectBackoffJitterMs),
                Thread::sleep);
    }

    /**
     * Creates a recovery strategy with injectable retry collaborators for deterministic tests.
     *
     * @param actions connector-owned recovery actions
     * @param transportConfig startup transport configuration containing reconnect backoff settings
     * @param maxReconnectAttempts maximum reconnect attempts per execution
     * @param jitterSupplier supplier used to derive bounded reconnect jitter
     * @param sleeper delay hook used between reconnect attempts
     * @throws NullPointerException if any dependency is null
     * @throws IllegalArgumentException if reconnect settings are invalid
     */
    CoinbaseL3RecoveryStrategy(
            Actions actions,
            TransportConfig transportConfig,
            int maxReconnectAttempts,
            LongSupplier jitterSupplier,
            Sleeper sleeper) {
        this.actions = Objects.requireNonNull(actions, "actions");
        Objects.requireNonNull(transportConfig, "transportConfig");
        if (transportConfig.reconnectBackoffBaseMs <= 0) {
            throw new IllegalArgumentException("transport.reconnectBackoffBaseMs must be positive");
        }
        if (transportConfig.reconnectBackoffMaxMs < transportConfig.reconnectBackoffBaseMs) {
            throw new IllegalArgumentException("transport.reconnectBackoffMaxMs must be >= reconnectBackoffBaseMs");
        }
        if (transportConfig.reconnectBackoffJitterMs < 0) {
            throw new IllegalArgumentException("transport.reconnectBackoffJitterMs must not be negative");
        }
        if (maxReconnectAttempts <= 0) {
            throw new IllegalArgumentException("maxReconnectAttempts must be positive");
        }
        this.baseBackoffMs = transportConfig.reconnectBackoffBaseMs;
        this.maxBackoffMs = transportConfig.reconnectBackoffMaxMs;
        this.jitterBoundMs = transportConfig.reconnectBackoffJitterMs;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.jitterSupplier = Objects.requireNonNull(jitterSupplier, "jitterSupplier");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /**
     * Sends the best-effort authenticated unsubscribe for the current Coinbase L3 subscription.
     *
     * @throws Exception if the connector cannot send the unsubscribe; the base strategy suppresses this failure
     */
    @Override
    protected void doUnsubscribe() throws Exception {
        actions.unsubscribe();
    }

    /**
     * Closes the current Coinbase WebSocket transport before reconnect.
     *
     * @throws Exception if the close step fails
     */
    @Override
    protected void doDisconnect() throws Exception {
        actions.close();
    }

    /**
     * Reconnects the Coinbase WebSocket channel using exponential backoff and jitter.
     *
     * @throws Exception if every reconnect attempt fails or the backoff sleep is interrupted
     */
    @Override
    protected void doReconnect() throws Exception {
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= maxReconnectAttempts; attempt++) {
            try {
                actions.reconnect();
                return;
            } catch (Exception ex) {
                lastFailure = ex;
                if (attempt == maxReconnectAttempts) {
                    throw ex;
                }
                sleeper.sleep(backoffDelayMs(attempt));
            }
        }
        throw lastFailure;
    }

    /**
     * Sends the fresh authenticated Coinbase L3 subscription after reconnect succeeds.
     *
     * @throws Exception if the connector cannot send the subscription
     */
    @Override
    protected void doResubscribe() throws Exception {
        actions.resubscribe();
    }

    /**
     * Records a best-effort unsubscribe failure without failing the recovery attempt.
     *
     * @param request immutable recovery request metadata
     * @param ctx attempt-scoped callbacks and counters
     * @param ex unsubscribe failure being suppressed
     */
    @Override
    protected void onBestEffortUnsubscribeFailure(RecoveryRequest request, RecoveryContext ctx, Exception ex) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(ex, "ex");
    }

    /**
     * Computes the reconnect delay for one failed attempt using exponential backoff plus bounded jitter.
     *
     * @param failedAttempt one-based failed reconnect attempt count
     * @return delay in milliseconds before the next reconnect attempt
     */
    private long backoffDelayMs(int failedAttempt) {
        long exponential = baseBackoffMs;
        for (int i = 1; i < failedAttempt && exponential < maxBackoffMs; i++) {
            exponential = Math.min(maxBackoffMs, exponential * 2L);
        }
        long jitter = jitterBoundMs == 0 ? 0 : Math.floorMod(jitterSupplier.getAsLong(), jitterBoundMs + 1L);
        return Math.min(maxBackoffMs + jitterBoundMs, exponential + jitter);
    }

    /**
     * Samples bounded reconnect jitter for production use.
     *
     * @param jitterBoundMs maximum jitter in milliseconds
     * @return random jitter between 0 and {@code jitterBoundMs}, inclusive
     */
    private static long nextJitter(long jitterBoundMs) {
        if (jitterBoundMs == 0) {
            return 0;
        }
        return ThreadLocalRandom.current().nextLong(jitterBoundMs + 1L);
    }

    /**
     * Connector-owned Coinbase channel actions used by the recovery strategy.
     */
    interface Actions {
        /**
         * Sends the authenticated unsubscribe payload for the current channel.
         *
         * @throws Exception if the send cannot be attempted
         */
        void unsubscribe() throws Exception;

        /**
         * Closes the current transport before reconnect.
         *
         * @throws Exception if the close step fails
         */
        void close() throws Exception;

        /**
         * Opens a fresh WebSocket connection.
         *
         * @throws Exception if the reconnect attempt fails
         */
        void reconnect() throws Exception;

        /**
         * Sends the authenticated L3 subscribe payload on the restored channel.
         *
         * @throws Exception if the send cannot be attempted
         */
        void resubscribe() throws Exception;
    }

    @FunctionalInterface
    interface Sleeper {
        /**
         * Sleeps for the computed retry delay.
         *
         * @param delayMs retry delay in milliseconds
         * @throws InterruptedException if the recovery thread is interrupted
         */
        void sleep(long delayMs) throws InterruptedException;
    }
}
