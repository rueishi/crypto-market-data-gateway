package io.rueishi.marketdata.crypto.core.bootstrap;

import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Runtime registry for connector factories discovered through Java {@link ServiceLoader}.
 *
 * <p>{@code VenueRegistry} is the only production bridge from {@code core} to
 * venue implementations. It loads {@link ConnectorFactory} providers from
 * classpath service metadata, indexes them by {@link VenueEnum}, and resolves
 * the one factory needed by {@link GatewayBootstrap} during startup. The class
 * intentionally imports only the connector SPI and shared config types; it must
 * never name a venue implementation class directly.</p>
 *
 * <p>Bootstrap typically constructs one registry at startup through the public
 * no-argument constructor. Tests in this package use the package-private
 * iterable constructor to verify duplicate and missing-factory behavior without
 * changing ServiceLoader metadata for the whole test JVM.</p>
 */
public final class VenueRegistry {
    private final Map<VenueEnum, ConnectorFactory> factories;

    /**
     * Discovers connector factories from the current thread context class loader.
     *
     * <p>Each provider listed in {@code META-INF/services} is instantiated by
     * {@link ServiceLoader} and indexed by the venue returned from
     * {@link ConnectorFactory#venue()}. Duplicate venue providers fail startup
     * immediately because bootstrap cannot choose between them safely.</p>
     *
     * @throws IllegalStateException if multiple factories declare the same venue
     */
    public VenueRegistry() {
        this(ServiceLoader.load(ConnectorFactory.class));
    }

    /**
     * Builds a registry from an explicit provider iterable.
     *
     * <p>This constructor keeps duplicate and missing-provider behavior
     * testable without changing process-wide ServiceLoader metadata. It follows
     * the same indexing rules as the public constructor: every factory must
     * declare a non-null venue and no venue may appear twice.</p>
     *
     * @param discoveredFactories provider iterable to index
     * @throws NullPointerException if the iterable, a factory, or a factory venue is null
     * @throws IllegalStateException if two factories declare the same venue
     */
    VenueRegistry(Iterable<ConnectorFactory> discoveredFactories) {
        Objects.requireNonNull(discoveredFactories, "discoveredFactories");
        EnumMap<VenueEnum, ConnectorFactory> map = new EnumMap<>(VenueEnum.class);
        for (ConnectorFactory factory : discoveredFactories) {
            Objects.requireNonNull(factory, "factory");
            VenueEnum venue = Objects.requireNonNull(factory.venue(), "factory.venue");
            ConnectorFactory previous = map.put(venue, factory);
            if (previous != null) {
                throw new IllegalStateException("Duplicate ConnectorFactory for venue: " + venue);
            }
        }
        this.factories = Collections.unmodifiableMap(map);
    }

    /**
     * Resolves the connector factory registered for a configured venue.
     *
     * <p>{@link GatewayBootstrap} calls this after configuration validation and
     * before connector creation. Missing venues fail with an actionable message
     * that points at the ServiceLoader provider metadata rather than falling
     * through to a null pointer later in startup.</p>
     *
     * @param venue configured venue/depth identity
     * @return registered factory for {@code venue}
     * @throws NullPointerException if {@code venue} is null
     * @throws IllegalArgumentException if no provider is registered for {@code venue}
     */
    public ConnectorFactory forVenue(VenueEnum venue) {
        Objects.requireNonNull(venue, "venue");
        ConnectorFactory factory = factories.get(venue);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "No ConnectorFactory registered for venue: " + venue
                            + ". Add a META-INF/services entry for "
                            + ConnectorFactory.class.getName());
        }
        return factory;
    }
}
