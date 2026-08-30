package dev.sbs.data;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot application entry point for the SkyBlock Simplified data service.
 *
 * <p>Holds the SkyBlock corpus in memory, watches its revision, and applies the writes the
 * deployment queues against it. No database is opened: the rows every repository serves come from
 * the corpus, and the Hazelcast client that remains carries the write queue rather than a
 * second-level cache.</p>
 *
 * <p>{@link EnableScheduling} activates the runner the corpus poll fires on. The
 * {@code dev.sbs.serverapi} package is scanned so the transitive server-api auto-configuration
 * registers; API key authentication stays off because the only endpoint served is
 * {@code /actuator/prometheus}.</p>
 */
@SpringBootApplication(scanBasePackages = { "dev.sbs.data", "dev.sbs.serverapi" })
@EnableScheduling
public class SimplifiedData {

    public static void main(String[] args) {
        SpringApplication.run(SimplifiedData.class, args);
    }

}
