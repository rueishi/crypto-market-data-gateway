package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test recovery strategy that can either complete Phase A or throw.
 */
final class ControlledRecoveryStrategy implements RecoveryStrategy {
    final AtomicInteger executeCount = new AtomicInteger();
    boolean throwFromExecute;

    @Override
    public void execute(RecoveryRequest request, RecoveryContext ctx) {
        executeCount.incrementAndGet();
        if (throwFromExecute) {
            throw new IllegalStateException("strategy failed");
        }
        ctx.onChannelRestored();
    }
}
