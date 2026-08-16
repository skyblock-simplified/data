package dev.sbs.data.config;

import api.simplified.github.GitHubAuth;
import api.simplified.github.GitHubContentsContract;
import api.simplified.github.exception.GitHubApiException;
import api.simplified.skyblock.SkyBlockFactory;
import api.simplified.skyblock.contract.SkyBlockDataContract;
import api.simplified.skyblock.contract.SkyBlockGitDataContract;
import dev.sbs.data.DataApi;
import dev.simplified.annotations.Log;
import dev.simplified.client.Client;
import dev.simplified.client.ClientConfig;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes the SkyBlock corpus clients as Spring beans.
 *
 * <p>The clients themselves, their auth, their media types and their error decoding belong to
 * {@link SkyBlockFactory#clients()}. This class only lifts them into the container, so the two
 * sides cannot drift: the read and write surfaces take different {@code Accept} headers, and a
 * second copy that misses the raw media type truncates a corpus file above one megabyte without
 * failing.
 *
 * <p>The Git Data client is the one thing built here, because the factory declares that contract
 * but has no use for it - the corpus is read through the Contents API and only this module's
 * write path speaks Git Data. It reads the same token the factory does.
 */
@Log
@Configuration
public class SkyBlockDataConfig {

    private static final @NotNull String GITHUB_JSON_ACCEPT = "application/vnd.github+json";
    private static final @NotNull String GITHUB_API_VERSION = "2022-11-28";

    /**
     * Registers the clients the corpus is served through.
     *
     * @return the read and write clients, and the contract aggregating them
     */
    @Bean
    public SkyBlockFactory.@NotNull Clients skyBlockClients() {
        log.info("Building SkyBlock corpus clients for source '{}'", SkyBlockFactory.SOURCE_ID);
        return SkyBlockFactory.clients();
    }

    /**
     * Registers the aggregated read and write façade the source adapters and the poller depend on.
     *
     * @param skyBlockClients the corpus clients
     * @return the aggregated data contract
     */
    @Bean
    public @NotNull SkyBlockDataContract skyBlockDataContract(SkyBlockFactory.@NotNull Clients skyBlockClients) {
        return skyBlockClients.contract();
    }

    /**
     * Registers the read client, whose last response carries the headers the poller conditions on.
     *
     * @param skyBlockClients the corpus clients
     * @return the read-side client wrapper
     */
    @Bean
    public @NotNull Client<GitHubContentsContract> gitHubContentsClient(SkyBlockFactory.@NotNull Clients skyBlockClients) {
        return skyBlockClients.read();
    }

    /**
     * Registers the index provider that holds the corpus manifest after its first load.
     *
     * @param skyBlockDataContract the read proxy bound to the data repository
     * @return an index provider over the manifest
     */
    @Bean
    public @NotNull IndexProvider gitHubIndexProvider(@NotNull SkyBlockDataContract skyBlockDataContract) {
        return SkyBlockFactory.indexProvider(SkyBlockFactory.SOURCE_ID, skyBlockDataContract, DataApi.getGson());
    }

    /**
     * Registers the file fetcher that reads one repo-root-relative path.
     *
     * @param skyBlockDataContract the read proxy bound to the data repository
     * @return a file fetcher over the corpus
     */
    @Bean
    public @NotNull FileFetcher gitHubFileFetcher(@NotNull SkyBlockDataContract skyBlockDataContract) {
        return SkyBlockFactory.fileFetcher(SkyBlockFactory.SOURCE_ID, skyBlockDataContract);
    }

    /**
     * Registers the Git Data client backing the write path.
     *
     * @return the Git Data API client wrapper
     */
    @Bean
    public @NotNull Client<SkyBlockGitDataContract> skyBlockGitDataClient() {
        GitHubAuth auth = GitHubAuth.bearer(StringUtil.stripToEmpty(System.getenv(SkyBlockFactory.TOKEN_VARIABLE)));
        GsonSettings gsonSettings = DataApi.getGsonSettings();
        ClientConfig<SkyBlockGitDataContract> options = ClientConfig.builder(SkyBlockGitDataContract.class, gsonSettings)
            .withHeader("Accept", GITHUB_JSON_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", auth)
            .withErrorDecoder(GitHubApiException::new)
            .build();

        log.info("Building SkyBlockGitDataContract client against api.github.com (Accept: {})", GITHUB_JSON_ACCEPT);
        return Client.create(options);
    }

    /**
     * Unwraps the Git Data contract proxy for direct injection into the write-path orchestrator.
     *
     * @param skyBlockGitDataClient the Git Data API client wrapper
     * @return the unwrapped Git Data proxy
     */
    @Bean
    public @NotNull SkyBlockGitDataContract skyBlockGitDataContract(
        @NotNull Client<SkyBlockGitDataContract> skyBlockGitDataClient
    ) {
        return skyBlockGitDataClient.getContract();
    }

}
