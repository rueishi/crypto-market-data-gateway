package io.rueishi.marketdata.crypto.core.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Gateway-level observability facade for the market-data gateway.
 *
 * <p>{@code GatewayCounters} is created once during gateway startup after the
 * process has initialized the shared Agrona {@link CountersManager}. Bootstrap
 * passes this facade into connector setup; each connector then asks it for an
 * {@link InstrumentCounters} bundle for its configured instrument. Gateway-wide
 * counters, such as connection attempts, live directly on this class while
 * connector/instrument counters live in {@code InstrumentCounters}.</p>
 *
 * <p>The relationship is intentionally thin: {@code CountersManager} performs
 * the low-level allocation, this facade applies gateway label dimensions and
 * owns counter lifecycle, and {@code InstrumentCounters} exposes stable
 * hot-path references to parser, encoder, publisher, recovery, and liveness
 * code. Runtime components should cache the returned counters and increment or
 * set them directly rather than allocating counters per message.</p>
 *
 * <p>The facade now sits inside {@link ObservabilityRuntime}, which owns the
 * mapped buffers and distinct error log. This class owns only the counters it
 * allocates, while exposing the underlying reader/manager handles needed by
 * generic metrics discovery and later runtime wiring.</p>
 */
public final class GatewayCounters implements AutoCloseable {
    private final CountersManager manager;
    private final String instanceId;
    private final String environment;
    private final String venue;
    private final List<AtomicCounter> ownedCounters = new ArrayList<>();

    private final AtomicCounter activeConnections;
    private final AtomicCounter connectionAttempts;
    private final AtomicCounter connectionSuccesses;
    private final AtomicCounter connectionFailures;
    private final AtomicCounter reconnectAttempts;

    /**
     * Creates the gateway-scoped counter facade and allocates gateway-level counters.
     *
     * @param manager Agrona counter allocator initialized by bootstrap
     * @param instanceId label value identifying this gateway process
     * @param environment label value identifying the runtime environment
     * @param venue label value identifying the configured venue
     * @throws NullPointerException if {@code manager}, {@code instanceId}, {@code environment}, or {@code venue} is null
     * @throws IllegalArgumentException if {@code instanceId}, {@code environment}, or {@code venue} is blank
     */
    public GatewayCounters(CountersManager manager, String instanceId, String environment, String venue) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.instanceId = requireText(instanceId, "instanceId");
        this.environment = requireText(environment, "environment");
        this.venue = requireText(venue, "venue");

        this.activeConnections = gatewayCounter("active_connections");
        this.connectionAttempts = gatewayCounter("connection_attempts");
        this.connectionSuccesses = gatewayCounter("connection_successes");
        this.connectionFailures = gatewayCounter("connection_failures");
        this.reconnectAttempts = gatewayCounter("reconnect_attempts");
    }

    /**
     * Allocates a new per-instrument counter bundle for a connector to cache during initialization.
     *
     * <p>The facade does not memoize bundles because connector ownership and
     * lifecycle are handled outside this class. Repeated calls for the same
     * instrument id therefore create distinct counter instances with identical
     * label dimensions.</p>
     *
     * @param instrumentId positive configured instrument id to include in counter labels
     * @return a newly allocated per-instrument counter bundle
     * @throws IllegalArgumentException if {@code instrumentId} is not positive
     */
    public InstrumentCounters forInstrument(int instrumentId) {
        if (instrumentId <= 0) {
            throw new IllegalArgumentException("instrumentId must be positive");
        }

        return new InstrumentCounters(this::instrumentCounter, instrumentId);
    }

    /**
     * Returns the shared counter manager used by this facade for allocation.
     *
     * <p>Runtime wiring and tests use this handle when they need the allocator
     * itself. Hot-path components should continue to work through stable
     * {@link AtomicCounter} references returned by this facade or
     * {@link InstrumentCounters}.</p>
     *
     * @return underlying Agrona counter manager
     */
    public CountersManager countersManager() {
        return manager;
    }

    /**
     * Returns a generic reader over all counters allocated in the shared manager.
     *
     * <p>Metrics export uses this reader-oriented API so new gateway and
     * instrument counters become visible without endpoint-specific
     * registration.</p>
     *
     * @return underlying Agrona counter reader
     */
    public CountersReader countersReader() {
        return manager;
    }

    /**
     * Returns the gateway-level active connection gauge.
     *
     * @return the pre-allocated active connection counter
     */
    public AtomicCounter activeConnections() {
        return activeConnections;
    }

    /**
     * Returns the gateway-level connection attempt counter.
     *
     * @return the pre-allocated connection attempt counter
     */
    public AtomicCounter connectionAttempts() {
        return connectionAttempts;
    }

    /**
     * Returns the gateway-level connection success counter.
     *
     * @return the pre-allocated connection success counter
     */
    public AtomicCounter connectionSuccesses() {
        return connectionSuccesses;
    }

    /**
     * Returns the gateway-level connection failure counter.
     *
     * @return the pre-allocated connection failure counter
     */
    public AtomicCounter connectionFailures() {
        return connectionFailures;
    }

    /**
     * Returns the gateway-level reconnect attempt counter.
     *
     * @return the pre-allocated reconnect attempt counter
     */
    public AtomicCounter reconnectAttempts() {
        return reconnectAttempts;
    }

    /**
     * Releases every counter allocated by this facade.
     *
     * <p>Gateway shutdown calls this method after connector work has stopped.
     * The method is safe to call repeatedly because owned references are cleared
     * after the first close pass.</p>
     */
    @Override
    public void close() {
        for (int i = ownedCounters.size() - 1; i >= 0; i--) {
            ownedCounters.get(i).close();
        }
        ownedCounters.clear();
    }

    /**
     * Allocates one gateway-scoped counter by logical name.
     *
     * @param name logical counter name without the {@code gateway_} prefix
     * @return a tracked Agrona counter labeled with gateway dimensions
     */
    private AtomicCounter gatewayCounter(String name) {
        return newCounter(label(name));
    }

    /**
     * Allocates one instrument-scoped counter by logical name.
     *
     * @param instrumentId configured instrument id to include in the label
     * @param name logical counter name without the {@code gateway_} prefix
     * @return a tracked Agrona counter labeled with gateway and instrument dimensions
     */
    private AtomicCounter instrumentCounter(int instrumentId, String name) {
        return newCounter(label(name) + ",instrumentId=" + instrumentId);
    }

    /**
     * Allocates and tracks a counter so facade shutdown can release it later.
     *
     * @param label complete Agrona counter label
     * @return the allocated counter
     */
    private AtomicCounter newCounter(String label) {
        AtomicCounter counter = manager.newCounter(label);
        ownedCounters.add(counter);
        return counter;
    }

    /**
     * Builds the shared label prefix for gateway and instrument counters.
     *
     * @param name logical counter name without the {@code gateway_} prefix
     * @return a deterministic label with gateway identity dimensions
     */
    private String label(String name) {
        return "gateway_" + name
                + ",instanceId=" + instanceId
                + ",environment=" + environment
                + ",venue=" + venue;
    }

    /**
     * Validates required label text before any counter uses it.
     *
     * @param value candidate label value
     * @param name field name used in validation errors
     * @return the validated text
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code value} is blank
     */
    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
