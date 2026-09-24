package dev.sbs.data.config;

import api.simplified.github.GitHubCorpus;
import api.simplified.github.GitHubToken;
import api.simplified.skyblock.SkyBlockData;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import dev.sbs.data.write.WriteQueueConsumer;
import dev.simplified.annotations.Log;
import jakarta.annotation.PreDestroy;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the SkyBlock corpus for {@code data}, with the instruction to write it back.
 *
 * <p>This service is the one deployment that holds a write token, so it is the one that asks the
 * corpus for its write half. Every other consumer reads. That difference is the token and nothing
 * else - no flag, no mode, no second source.
 *
 * <p>There is no session and no driver. Nothing here reads the corpus except to write it - the
 * {@link WriteQueueConsumer} applies each write through {@link SkyBlockData#writing(GitHubCorpus)} -
 * so no generation is held, no database is opened and no second-level cache exists to configure.
 * The Hazelcast client that remains is the write queue's, not Hibernate's.
 */
@Configuration
@Log
public class PersistenceConfig {

    /**
     * The environment variable holding the GitHub personal access token the corpus is written with.
     */
    public static final @NotNull String TOKEN_VARIABLE = "SKYBLOCK_GITHUB_TOKEN";

    /**
     * The write-path Hazelcast client, held so {@link #shutdownWriteHazelcastInstance()} can close
     * it on context teardown.
     */
    private volatile HazelcastInstance writeHazelcastInstance;

    /**
     * The corpus this service reads and writes.
     *
     * <p>The token is read from {@value #TOKEN_VARIABLE} and an unset one is answered here, at
     * startup, rather than as a rejected write later. It authenticates every read too - the
     * catalogue refresh and the layer reads each write merges before it rewrites a document - which
     * is what lifts them off the sixty-an-hour cap an anonymous client works under.
     *
     * @return the corpus
     */
    @Bean
    public @NotNull GitHubCorpus skyBlockCorpus() {
        return SkyBlockData.corpus()
            .token(GitHubToken.of(TOKEN_VARIABLE))
            .build();
    }

    /**
     * The Hazelcast client the write queue and its retry map live on.
     *
     * @return a fresh client connected to the cluster the classpath config names
     */
    @Bean
    public @NotNull HazelcastInstance skyBlockWriteHazelcastInstance() {
        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        this.writeHazelcastInstance = instance;
        log.info(
            "data write path: Hazelcast client '{}' connected to cluster '{}'",
            instance.getName(), instance.getConfig().getClusterName()
        );
        return instance;
    }

    @PreDestroy
    void shutdownWriteHazelcastInstance() {
        HazelcastInstance instance = this.writeHazelcastInstance;

        if (instance != null) {
            try {
                instance.shutdown();
                log.info("data write path: Hazelcast client '{}' shut down cleanly", instance.getName());
            } catch (Throwable ex) {
                log.warn("data write path: Hazelcast client shutdown raised exception (ignored)", ex);
            }
        }
    }

}
