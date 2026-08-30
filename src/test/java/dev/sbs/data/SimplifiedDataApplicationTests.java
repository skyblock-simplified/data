package dev.sbs.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Spring Boot context-loads test for {@link SimplifiedData}.
 *
 * <p>Starting the context connects to a Hazelcast cluster, reads a write token and fetches the
 * corpus catalogue, so this runs only where all three are available. Set
 * {@code SKYBLOCK_HAZELCAST=true} to run it; everywhere else it reports as skipped rather than
 * failing on an absence it cannot do anything about.</p>
 */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "SKYBLOCK_HAZELCAST", matches = "true")
class SimplifiedDataApplicationTests {

    @Test
    void contextLoads() {
        // Empty body; the annotation is the assertion. A context that cannot start fails here
        // with the underlying cause.
    }

}
