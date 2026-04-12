package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;

/**
 * Connector lifecycle contract for one venue/instrument stream.
 *
 * <p>Bootstrap obtains a {@code Connector} from a {@link ConnectorFactory},
 * calls {@link #init(ConnectorContext)} once with shared gateway resources, and
 * then calls {@link #connect()} to start the stream. The connector owns the
 * event-loop-facing parser, encoder, publisher references, session contexts,
 * and recovery state for exactly one configured instrument.</p>
 *
 * <p>Implementations must unpack {@link ConnectorContext} during initialization
 * and must not keep the context wrapper as a field. Recovery requests are
 * routed to the connector that owns the request instrument and are executed on
 * the owning event-loop path.</p>
 */
public interface Connector {

    /**
     * Initializes the connector from bootstrap-owned dependencies.
     *
     * <p>This method is called once after construction and before
     * {@link #connect()}, frame handling, or recovery. Implementations unpack the
     * context into connector-owned fields, allocate the encoder and session
     * contexts, and then discard the wrapper.</p>
     *
     * @param ctx dependency carrier created by bootstrap for this connector
     */
    void init(ConnectorContext ctx);

    /**
     * Starts the connector stream after initialization.
     *
     * <p>Phase 1 keeps live transport out of scope, so base implementations may
     * perform only subscription/snapshot lifecycle hooks. Later transport-backed
     * connectors can add network behavior behind the same lifecycle call.</p>
     */
    void connect();

    /**
     * Runs a recovery attempt for this connector.
     *
     * <p>The connector publishes {@code BOOK_RESET}, coalesces duplicate
     * requests while recovery is in progress, and delegates channel work to its
     * recovery strategy. The recovery-in-progress flag remains set until the
     * snapshot context reports boundary acceptance.</p>
     *
     * @param request recovery request metadata for this connector
     */
    void recover(RecoveryRequest request);

    /**
     * Schedules or submits a recovery request through the connector's ownership path.
     *
     * <p>Downstream routers call this method instead of directly invoking
     * {@link #recover(RecoveryRequest)} so transport-backed connectors can run
     * recovery on their dedicated event-loop thread. Simple test or placeholder
     * connectors may rely on this default implementation, which executes
     * recovery synchronously.</p>
     *
     * @param request recovery request metadata for this connector
     * @throws NullPointerException if {@code request} is null
     */
    default void requestRecovery(RecoveryRequest request) {
        recover(java.util.Objects.requireNonNull(request, "request"));
    }

    /**
     * Stops connector work and performs best-effort shutdown actions.
     *
     * <p>Phase 1 shutdown is intentionally a skeleton because live transport is
     * deferred. The method still exposes the lifecycle boundary later bootstrap
     * code will call.</p>
     */
    void shutdown();

    /**
     * Returns the venue/depth identity served by this connector.
     *
     * @return configured venue identity
     */
    VenueEnum venue();

    /**
     * Returns the stable internal instrument id served by this connector.
     *
     * @return configured instrument id
     */
    int instrumentId();
}
