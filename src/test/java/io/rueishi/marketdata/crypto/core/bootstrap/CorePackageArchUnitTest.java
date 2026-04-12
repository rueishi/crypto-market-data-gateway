package io.rueishi.marketdata.crypto.core.bootstrap;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import org.junit.jupiter.api.Test;

/**
 * ArchUnit tests for the core-to-venue package boundary.
 *
 * <p>The gateway keeps exchange-specific code under {@code venue} and shared
 * runtime infrastructure under {@code core}. This test imports the project
 * classes, including the test-only stub venue package, and verifies that core
 * classes do not take compile-time dependencies on venue classes and that
 * venue implementations do not retain connector/runtime context wrappers as
 * fields. The rules are executable guards for the package and lifecycle
 * constraints behind ServiceLoader extensibility and event-loop-owned
 * recovery.</p>
 */
class CorePackageArchUnitTest {

    /**
     * Verifies that core classes do not import or otherwise depend on venue package classes.
     */
    @Test
    void coreDoesNotDependOnVenuePackages() {
        JavaClasses classes = new ClassFileImporter().importPackages("io.rueishi.marketdata.crypto");

        noClasses()
                .that()
                .resideInAPackage("..core..")
                .should()
                .dependOnClassesThat()
                .resideInAPackage("..venue..")
                .check(classes);
    }

    /**
     * Verifies that venue classes receive context wrappers only as call-scoped
     * parameters and do not retain them beyond the intended lifecycle boundary.
     */
    @Test
    void venueClassesDoNotRetainContextFields() {
        JavaClasses classes = new ClassFileImporter().importPackages("io.rueishi.marketdata.crypto");

        noFields()
                .that()
                .areDeclaredInClassesThat()
                .resideInAPackage("..venue..")
                .should()
                .haveRawType(ConnectorContext.class)
                .orShould()
                .haveRawType(ParseContext.class)
                .orShould()
                .haveRawType(SnapshotContext.class)
                .orShould()
                .haveRawType(RecoveryContext.class)
                .check(classes);
    }
}
