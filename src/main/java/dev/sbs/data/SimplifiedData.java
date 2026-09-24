package dev.sbs.data;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application entry point for the SkyBlock Simplified data service.
 *
 * <p>Holds the SkyBlock corpus in memory and applies the writes the deployment queues against it.
 * No database is opened: the rows every repository serves come from the corpus, which the session
 * keeps current on the cadence the corpus models declare, and the Hazelcast client that remains
 * carries the write queue rather than a second-level cache.</p>
 *
 * <p>The {@code dev.sbs.serverapi} package is scanned so the transitive server-api
 * auto-configuration registers; API key authentication stays off because the only endpoint served
 * is {@code /actuator/prometheus}.</p>
 */
@SpringBootApplication(scanBasePackages = { "dev.sbs.data", "dev.sbs.serverapi" })
public class SimplifiedData {

    public static void main(String[] args) {
        SpringApplication.run(SimplifiedData.class, args);
    }

}
