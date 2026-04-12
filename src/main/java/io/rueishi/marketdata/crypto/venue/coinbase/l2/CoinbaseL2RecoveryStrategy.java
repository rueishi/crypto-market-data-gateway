package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.recovery.ReconnectRecoveryStrategy;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Coinbase L2 full reconnect recovery strategy.
 *
 * <p>{@code CoinbaseL2RecoveryStrategy} is the Phase 3 venue strategy invoked
 * by {@link io.rueishi.marketdata.crypto.core.connector.AbstractConnector}
 * after a recovery request has passed venue/instrument filtering and published
 * {@code BOOK_RESET}. It delegates Coinbase transport details through a narrow
 * connector-owned {@link Actions} interface so the strategy owns ordering and
 * backoff while {@link CoinbaseL2Connector} owns the actual WebSocket channel
 * and authenticated subscribe/unsubscribe sends.</p>
 *
 * <p>All Coinbase L2 recovery request types use the same full path because the
 * feed has no snapshot-on-demand request in this gateway model: best-effort
 * unsubscribe, close, reconnect with exponential backoff and jitter,
 * authenticated resubscribe, then
 * {@link RecoveryContext#onChannelRestored()} to let the connector reset
 * session state and await a fresh feed snapshot boundary.</p>
 */
public final class CoinbaseL2RecoveryStrategy extends ReconnectRecoveryStrategy {
    private static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;

    private final Actions actions;
    private final long baseBackoffMs;
    private final long maxBackoffMs;
    private final long jitterBoundMs;
    private final int maxReconnectAttempts;
    private final LongSupplier jitterSupplier;
    private final Sleeper sleeper;

    /**
     * Creates a production Coinbase recovery strategy from connector actions and transport config.
     *
     * @param actions connector-owned recovery actions
     * @param transportConfig startup transport configuration containing reconnect backoff settings
     * @throws NullPointerException if {@code actions} or {@code transportConfig} is null
     * @throws IllegalArgumentException if reconnect timing settings are invalid
     */
    public CoinbaseL2RecoveryStrategy(Actions actions, TransportConfig transportConfig) {
        this(
                actions,
                transportConfig,
                DEFAULT_MAX_RECONNECT_ATTEMPTS,
                () -> nextJitter(transportConfig.reconnectBackoffJitterMs),
                Thread::sleep);
    }

    CoinbaseL2RecoveryStrategy(
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
     * Sends the best-effort authenticated unsubscribe for the current Coinbase subscription.
     *
     * @throws Exception if the connector cannot send the unsubscribe; the base
     * strategy suppresses this failure and continues with close/reconnect
     */
    @Override
    protected void doUnsubscribe() throws Exception {
        actions.unsubscribe();
    }

    /**
     * Closes the current Coinbase WebSocket transport before reconnect.
     */
    @Override
    protected void doDisconnect() {
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
     * Sends the fresh authenticated Coinbase subscribe payload after reconnect succeeds.
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
     * <p>The current observability context does not expose the gateway error
     * log, so this hook keeps recovery-failure counters reserved for attempts
     * that actually fail to restore the channel.</p>
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

    private long backoffDelayMs(int failedAttempt) {
        long exponential = baseBackoffMs;
        for (int i = 1; i < failedAttempt && exponential < maxBackoffMs; i++) {
            exponential = Math.min(maxBackoffMs, exponential * 2L);
        }
        long jitter = jitterBoundMs == 0 ? 0 : Math.floorMod(jitterSupplier.getAsLong(), jitterBoundMs + 1L);
        return Math.min(maxBackoffMs + jitterBoundMs, exponential + jitter);
    }

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
         */
        void close();

        /**
         * Opens a fresh WebSocket connection.
         *
         * @throws Exception if the reconnect attempt fails
         */
        void reconnect() throws Exception;

        /**
         * Sends the authenticated subscribe payload on the restored channel.
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
