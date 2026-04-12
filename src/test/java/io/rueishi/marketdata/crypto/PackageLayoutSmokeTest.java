package io.rueishi.marketdata.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Structural smoke test for the project package layout.
 *
 * <p>The gateway architecture relies on stable package roots: {@code core/}
 * contains shared infrastructure, production {@code venue/} code sits behind
 * ServiceLoader, and Phase 2 Coinbase L2 wiring is present without reshaping
 * the Maven layout.</p>
 */
class PackageLayoutSmokeTest {
    private static final Path PROJECT_ROOT = Path.of("").toAbsolutePath();

    /**
     * Verifies that the production and test package roots promised by Phase 1 are present.
     */
    @Test
    void requiredPhaseOneCorePackageRootsExist() {
        List<String> requiredDirectories = List.of(
                "src/main/java/io/rueishi/marketdata/crypto/core/bootstrap",
                "src/main/java/io/rueishi/marketdata/crypto/core/config",
                "src/main/java/io/rueishi/marketdata/crypto/core/connector",
                "src/main/java/io/rueishi/marketdata/crypto/core/encoding",
                "src/main/java/io/rueishi/marketdata/crypto/core/observability",
                "src/main/java/io/rueishi/marketdata/crypto/core/parser",
                "src/main/java/io/rueishi/marketdata/crypto/core/publisher",
                "src/main/java/io/rueishi/marketdata/crypto/core/recovery",
                "src/main/java/io/rueishi/marketdata/crypto/core/sequence",
                "src/main/java/io/rueishi/marketdata/crypto/core/snapshot",
                "src/main/java/io/rueishi/marketdata/crypto/core/subscription",
                "src/main/java/io/rueishi/marketdata/crypto/core/transport",
                "src/test/java/io/rueishi/marketdata/crypto/core/bootstrap",
                "src/test/java/io/rueishi/marketdata/crypto/core/config",
                "src/test/java/io/rueishi/marketdata/crypto/core/connector",
                "src/test/java/io/rueishi/marketdata/crypto/core/encoding",
                "src/test/java/io/rueishi/marketdata/crypto/core/observability",
                "src/test/java/io/rueishi/marketdata/crypto/core/parser",
                "src/test/java/io/rueishi/marketdata/crypto/core/publisher",
                "src/test/java/io/rueishi/marketdata/crypto/core/recovery",
                "src/test/java/io/rueishi/marketdata/crypto/core/sequence",
                "src/test/java/io/rueishi/marketdata/crypto/core/snapshot");

        assertThat(requiredDirectories)
                .allSatisfy(relativePath ->
                        assertThat(PROJECT_ROOT.resolve(relativePath))
                                .describedAs(relativePath)
                                .isDirectory());
    }

    /**
     * Verifies that the ConnectorFactory ServiceLoader bridge registers Coinbase L2 for Phase 2.
     *
     * @throws Exception if the ServiceLoader file cannot be read
     */
    @Test
    void serviceLoaderBridgeFileRegistersCoinbaseL2Provider() throws Exception {
        Path serviceFile = PROJECT_ROOT.resolve(
                "src/main/resources/META-INF/services/io.rueishi.marketdata.crypto.core.connector.ConnectorFactory");

        assertThat(serviceFile).isRegularFile();
        assertThat(Files.readAllLines(serviceFile).stream()
                        .map(String::trim)
                        .filter(line -> !line.isEmpty())
                        .filter(line -> !line.startsWith("#")))
                .containsExactly("io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2ConnectorFactory");
    }

    /**
     * Verifies that Phase 2 Coinbase L2 connector implementations are present and L3 remains deferred.
     */
    @Test
    void phaseTwoCoinbaseL2ConnectorImplementationsExist() {
        assertThat(PROJECT_ROOT.resolve("src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/shared"))
                .isDirectory();
        assertThat(PROJECT_ROOT.resolve(
                "src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2SubscriptionBuilder.java"))
                .isRegularFile();
        assertThat(PROJECT_ROOT.resolve(
                "src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2Connector.java"))
                .isRegularFile();
        assertThat(PROJECT_ROOT.resolve(
                "src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l2/CoinbaseL2ConnectorFactory.java"))
                .isRegularFile();
        assertThat(PROJECT_ROOT.resolve("src/main/java/io/rueishi/marketdata/crypto/venue/coinbase/l3"))
                .doesNotExist();
    }
}
