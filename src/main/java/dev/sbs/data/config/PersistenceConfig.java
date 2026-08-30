package dev.sbs.data.config;

import api.simplified.github.GitHubCorpus;
import api.simplified.github.GitHubToken;
import api.simplified.skyblock.SkyBlockFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import dev.sbs.data.DataApi;
import dev.simplified.annotations.Log;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.store.Source;
import dev.simplified.util.Logging;
import jakarta.annotation.PreDestroy;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the SkyBlock corpus for {@code data}, with the instruction to write it back.
 *
 * <p>This service is the one deployment that holds a write token, so it is the one that asks the
 * corpus for its write half. Every other consumer reads. That difference is the token and nothing
 * else - no flag, no mode, no second factory.
 *
 * <p>There is one session and it holds no driver. The rows every repository serves come from the
 * corpus, so no database is opened, no schema is created and no second-level cache exists to
 * configure. The Hazelcast client that remains is the write queue's, not Hibernate's.
 */
@Configuration
@Log
public class PersistenceConfig {

    /**
     * The write-path Hazelcast client, held so {@link #shutdownWriteHazelcastInstance()} can close
     * it on context teardown.
     */
    private volatile HazelcastInstance writeHazelcastInstance;

    /**
     * The corpus this service reads and writes.
     *
     * @return the corpus
     */
    @Bean
    public @NotNull GitHubCorpus skyBlockCorpus() {
        return SkyBlockFactory.CORPUS;
    }

    /**
     * The corpus session, holding a generation of every registered type.
     *
     * <p>The write instruction is read from {@value SkyBlockFactory#TOKEN_VARIABLE}. Without one
     * the source is read-only, and {@link JpaSession#write} refuses rather than half-succeeding,
     * which is what makes an unset token a startup-time answer rather than a runtime surprise.
     *
     * @param skyBlockCorpus the corpus
     * @return the session
     */
    @Bean
    public @NotNull JpaSession skyBlockSession(@NotNull GitHubCorpus skyBlockCorpus) {
        Source source = skyBlockCorpus.writing(GitHubToken.of(SkyBlockFactory.TOKEN_VARIABLE));

        JpaSession session = new SessionManager().connect(
            JpaConfig.builder()
                .withRepositoryFactory(new SkyBlockFactory(source))
                .withGsonSettings(
                    DataApi.getGsonSettings()
                        .mutate()
                        .withStringType(GsonSettings.StringType.DEFAULT)
                        .build()
                )
                .withLogLevel(Logging.Level.WARN)
                .build()
        );

        log.info("data corpus session wired against '{}' with a write instruction", skyBlockCorpus);
        return session;
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
