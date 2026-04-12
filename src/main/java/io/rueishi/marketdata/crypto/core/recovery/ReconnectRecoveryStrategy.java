package io.rueishi.marketdata.crypto.core.recovery;

import java.util.Objects;

/**
 * Template base class for recovery that reconnects and resubscribes the venue channel.
 *
 * <p>Most venue implementations recover by unsubscribing, disconnecting,
 * reconnecting, and then sending a fresh subscription. This class centralizes
 * that order and the {@link RecoveryContext} terminal-callback contract so
 * concrete venue strategies only implement the venue-specific channel steps.
 * It is constructed during connector wiring and invoked by connector recovery
 * flow for each {@link RecoveryRequest}.</p>
 *
 * <p>The base class is stateless and does not retain {@link RecoveryRequest} or
 * {@link RecoveryContext}. A successful sequence signals
 * {@link RecoveryContext#onChannelRestored()} exactly once. The unsubscribe
 * step is best-effort, because the old channel may already be gone when
 * recovery begins; disconnect, reconnect, and resubscribe failures are
 * converted into one recovery-failed signal.</p>
 */
public abstract class ReconnectRecoveryStrategy implements RecoveryStrategy {

    /**
     * Executes the reconnect template and emits exactly one terminal recovery callback.
     *
     * <p>The method increments recovery execution counters before the venue
     * steps begin. Exceptions from the venue steps are converted into a failure
     * signal; success signals that channel work is complete and snapshot
     * acquisition can begin.</p>
     *
     * @param request immutable recovery request metadata
     * @param ctx attempt-scoped callbacks and counters
     * @throws NullPointerException if {@code request} or {@code ctx} is null
     */
    @Override
    public final void execute(RecoveryRequest request, RecoveryContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");

        try {
            ctx.counters().recoveryExecutions().increment();
            doBestEffortUnsubscribe(request, ctx);
            doDisconnect();
            doReconnect();
            doResubscribe();
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            ctx.counters().recoveryFailures().increment();
            ctx.onRecoveryFailed(failureReason(ex));
            return;
        }

        ctx.onChannelRestored();
    }

    /**
     * Sends a best-effort venue unsubscribe before closing the channel.
     *
     * @throws Exception when the venue-specific unsubscribe step cannot complete
     */
    protected abstract void doUnsubscribe() throws Exception;

    /**
     * Closes the current venue channel.
     *
     * @throws Exception when the venue-specific disconnect step cannot complete
     */
    protected abstract void doDisconnect() throws Exception;

    /**
     * Reconnects to the venue, potentially using venue-specific backoff and authentication.
     *
     * @throws Exception when the venue-specific reconnect step cannot complete
     */
    protected abstract void doReconnect() throws Exception;

    /**
     * Sends the fresh venue subscription after reconnect succeeds.
     *
     * @throws Exception when the venue-specific resubscribe step cannot complete
     */
    protected abstract void doResubscribe() throws Exception;

    /**
     * Handles a suppressed best-effort unsubscribe failure.
     *
     * <p>Concrete strategies may override this hook to record diagnostics. The
     * default implementation deliberately avoids incrementing recovery-failure
     * counters because the recovery attempt may still complete successfully
     * after close, reconnect, and resubscribe.</p>
     *
     * @param request immutable recovery request metadata
     * @param ctx attempt-scoped callbacks and counters
     * @param ex unsubscribe failure being suppressed
     */
    protected void onBestEffortUnsubscribeFailure(RecoveryRequest request, RecoveryContext ctx, Exception ex) {
    }

    private void doBestEffortUnsubscribe(RecoveryRequest request, RecoveryContext ctx) throws InterruptedException {
        try {
            doUnsubscribe();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (Exception ex) {
            onBestEffortUnsubscribeFailure(request, ctx, ex);
        }
    }

    private static String failureReason(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            return ex.getClass().getSimpleName();
        }
        return message;
    }
}
