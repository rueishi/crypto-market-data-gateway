package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import org.junit.jupiter.api.Test;

/**
 * Subscription acknowledgement tests for {@link CoinbaseL2FeedParser}.
 *
 * <p>The tests exercise Coinbase L2 control-plane validation without involving a
 * connector recovery strategy. Required Coinbase channels and product ids are
 * validated through the parser context; invalid acknowledgements increment
 * counters and request connector-owned recovery.</p>
 */
class CoinbaseL2SubscriptionsAckTest {

    /**
     * Verifies that a valid acknowledgement marks the session and seeds heartbeat timing.
     */
    @Test
    void validatesRequiredChannelsAndProductId() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"subscriptions","channels":[
                {"name":"level2","product_ids":["BTC-USD"]},
                {"name":"heartbeat","product_ids":["BTC-USD"]}]}""");

        assertThat(support.context.subscriptionAckValidated()).isTrue();
        assertThat(support.counters.lastHeartbeatReceivedNanos().get()).isEqualTo(9_876_543_210L);
        assertThat(support.counters.subscriptionValidationFailures().get()).isZero();
        assertThat(support.publisher.totalCount()).isZero();
        assertThat(support.recoverySignals).isEmpty();
    }

    /**
     * Verifies that extra channels are tolerated while required channels still pass.
     */
    @Test
    void toleratesExtraChannelsInAcknowledgement() {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse("""
                {"type":"subscriptions","channels":[
                {"name":"ticker","product_ids":["BTC-USD"]},
                {"name":"level2","product_ids":["BTC-USD"]},
                {"name":"heartbeat","product_ids":["BTC-USD"]}]}""");

        assertThat(support.context.subscriptionAckValidated()).isTrue();
        assertThat(support.counters.subscriptionValidationFailures().get()).isZero();
        assertThat(support.recoverySignals).isEmpty();
    }

    /**
     * Verifies bad acknowledgements are counted and request reset recovery.
     */
    @Test
    void rejectsMissingChannelOrWrongProductAndRequestsRecovery() {
        CoinbaseL2ParserTestSupport missingChannel = new CoinbaseL2ParserTestSupport();
        CoinbaseL2ParserTestSupport wrongProduct = new CoinbaseL2ParserTestSupport();

        missingChannel.parse("""
                {"type":"subscriptions","channels":[
                {"name":"level2","product_ids":["BTC-USD"]}]}""");
        wrongProduct.parse("""
                {"type":"subscriptions","channels":[
                {"name":"level2","product_ids":["ETH-USD"]},
                {"name":"heartbeat","product_ids":["ETH-USD"]}]}""");

        assertThat(missingChannel.context.subscriptionAckValidated()).isFalse();
        assertThat(missingChannel.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(wrongProduct.context.subscriptionAckValidated()).isFalse();
        assertThat(wrongProduct.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(wrongProduct.publisher.totalCount()).isZero();
        assertThat(missingChannel.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                    assertThat(signal.diagnosticText()).contains("subscriptions");
                });
        assertThat(wrongProduct.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                });
    }
}
