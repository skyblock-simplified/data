package dev.sbs.data;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application entry point for the SkyBlock Simplified data service.
 *
 * <p>Applies the writes the deployment queues against the SkyBlock corpus. It holds no session and
 * reads the corpus only to write it: each write merges the documents it rewrites and commits them
 * back. No database is opened, and the Hazelcast client carries the write queue rather than a
 * second-level cache.</p>
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
