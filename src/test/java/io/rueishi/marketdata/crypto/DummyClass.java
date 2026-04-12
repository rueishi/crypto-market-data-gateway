package io.rueishi.marketdata.crypto;

/**
 * Minimal test helper used by {@link BuildSanityTest}.
 *
 * <p>This class has no runtime role in the gateway. Its only purpose is to
 * provide a stable classpath target that the build sanity test can load via
 * reflection, proving the Maven test classpath, package naming, and test
 * compilation pipeline are wired correctly before later Phase 1 cards add real
 * runtime classes.</p>
 */
final class DummyClass {

    private DummyClass() {
    }
}
