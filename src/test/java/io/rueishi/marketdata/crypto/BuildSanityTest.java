package io.rueishi.marketdata.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Build-level smoke test for the initial Maven skeleton.
 *
 * <p>This test does not validate gateway business behavior. It verifies the
 * earliest build flow: Maven compiles test classes, Surefire launches JUnit 5,
 * and the JVM can load a class from the project test package. Later Phase 1
 * tests build on this foundation.</p>
 */
class BuildSanityTest {

    /**
     * Verifies that the Maven test runtime can load a compiled test helper by fully qualified class name.
     *
     * @throws Exception if the helper class cannot be loaded from the test classpath
     */
    @Test
    void testRuntimeCanLoadDummyClass() throws Exception {
        assertThat(Class.forName("io.rueishi.marketdata.crypto.DummyClass"))
                .isSameAs(DummyClass.class);
    }
}
