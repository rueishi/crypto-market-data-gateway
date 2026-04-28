package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.bootstrap.VenueRegistry;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Structural completeness checks for the Phase 2 Coinbase L2 package.
 *
 * <p>The tests verify that the production {@code venue/coinbase/l2} package
 * contains the expected connector, factory, parser, subscription, snapshot,
 * and recovery classes, and that Java ServiceLoader metadata resolves the
 * {@link CoinbaseL2ConnectorFactory} through the shared core
 * {@link VenueRegistry}. No connector is initialized and no network transport
 * is opened; this suite is focused on packaging and startup wiring.</p>
 */
class CoinbaseL2PackageCompletenessTest {
    private static final Path PROJECT_ROOT = Path.of("").toAbsolutePath();
    private static final Path L2_SOURCE_DIR =
            PROJECT_ROOT.resolve("src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2");
    private static final Path SERVICE_FILE = PROJECT_ROOT.resolve(
            "src/main/resources/META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory");

    /**
     * Verifies every production class required by the Coinbase L2 strategy is present on disk.
     */
    @Test
    void coinbaseL2ProductionPackageContainsCompletePhaseTwoImplementationSet() {
        assertThat(L2_SOURCE_DIR).isDirectory();
        assertThat(List.of(
                "CoinbaseL2Connector.java",
                "CoinbaseL2ConnectorFactory.java",
                "CoinbaseL2FeedParser.java",
                "CoinbaseL2RecoveryStrategy.java",
                "CoinbaseL2SnapshotStrategy.java",
                "CoinbaseL2SubscriptionBuilder.java"))
                .allSatisfy(fileName -> assertThat(L2_SOURCE_DIR.resolve(fileName))
                        .describedAs(fileName)
                        .isRegularFile());
    }

    /**
     * Verifies the package classes are loadable and retain the expected type relationships.
     */
    @Test
    void coinbaseL2ClassesHaveExpectedRuntimeTypes() {
        assertThat(CoinbaseL2Connector.class)
                .isAssignableTo(io.rueishi.marketdata.crypto.core.connector.Connector.class);
        assertThat(CoinbaseL2ConnectorFactory.class).isAssignableTo(ConnectorFactory.class);
        assertThat(CoinbaseL2FeedParser.class)
                .isAssignableTo(io.rueishi.marketdata.crypto.core.parser.FeedParser.class);
        assertThat(CoinbaseL2SnapshotStrategy.class)
                .isAssignableTo(io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy.class);
        assertThat(CoinbaseL2RecoveryStrategy.class)
                .isAssignableTo(io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy.class);
        assertThat(CoinbaseL2SubscriptionBuilder.class)
                .isAssignableTo(io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder.class);
    }

    /**
     * Verifies ServiceLoader metadata and the core registry resolve Coinbase L2 end to end.
     *
     * @throws Exception if the service metadata cannot be read
     */
    @Test
    void serviceRegistrationAndVenueRegistryResolveCoinbaseL2() throws Exception {
        assertThat(Files.readAllLines(SERVICE_FILE).stream()
                        .map(String::trim)
                        .filter(line -> !line.isEmpty())
                        .filter(line -> !line.startsWith("#")))
                .contains(CoinbaseL2ConnectorFactory.class.getName());

        ConnectorFactory factory = new VenueRegistry().forVenue(VenueEnum.COINBASE_L2);

        assertThat(factory).isInstanceOf(CoinbaseL2ConnectorFactory.class);
        assertThat(factory.venue()).isEqualTo(VenueEnum.COINBASE_L2);
    }
}
