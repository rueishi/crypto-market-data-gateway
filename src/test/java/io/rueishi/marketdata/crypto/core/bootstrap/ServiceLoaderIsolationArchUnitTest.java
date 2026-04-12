package io.rueishi.marketdata.crypto.core.bootstrap;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for keeping {@link VenueRegistry} isolated from concrete venue implementations.
 *
 * <p>The registry is allowed to know only the shared connector SPI and Java
 * {@link java.util.ServiceLoader}. These tests combine an ArchUnit dependency
 * rule with a source-level guard that catches accidental hard-coded venue class
 * names in string form, which bytecode dependency checks would not see.</p>
 */
class ServiceLoaderIsolationArchUnitTest {
    private static final Path VENUE_REGISTRY_SOURCE = Path.of(
            "src/main/java/io/rueishi/marketdata/crypto/core/bootstrap/VenueRegistry.java");

    /**
     * Verifies that VenueRegistry has no bytecode dependency on classes from the venue package.
     */
    @Test
    void venueRegistryDoesNotDependOnVenuePackages() {
        JavaClasses classes = new ClassFileImporter().importPackages("io.rueishi.marketdata.crypto");

        noClasses()
                .that()
                .haveFullyQualifiedName(VenueRegistry.class.getName())
                .should()
                .dependOnClassesThat()
                .resideInAPackage("..venue..")
                .check(classes);
    }

    /**
     * Verifies that the registry source keeps ServiceLoader as its discovery mechanism.
     *
     * @throws Exception if the source file cannot be read
     */
    @Test
    void venueRegistrySourceNamesServiceLoaderButNoConcreteVenuePackage() throws Exception {
        String source = Files.readString(VENUE_REGISTRY_SOURCE);

        assertThat(source).contains("ServiceLoader.load");
        assertThat(source).doesNotContain("io.rueishi.marketdata.crypto.venue.");
    }
}
