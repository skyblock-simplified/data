package dev.sbs.data.poller;

import api.simplified.github.GitHubCorpus;
import api.simplified.github.ManifestIndex;
import dev.simplified.annotations.Log;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Watches the corpus revision and records which documents moved under it.
 *
 * <p>A poll is one request: the revision either moved or it did not, and a revision that did not
 * move cannot have moved anything under it. When it did, the corpus takes a fresh catalogue and
 * this compares each document's composed fingerprint against the one it last saw.
 *
 * <p>What it does not do is rebuild anything. The catalogue the corpus now holds is the one the
 * next hydration reads through, and when that happens is the hydrator's decision - a poller that
 * could force a rebuild would be a way for a consumer to force one, which is the thing the design
 * refuses. Calling this in a loop causes zero rehydrations.
 *
 * <p>The fingerprints live here, in memory. Their only value is saving work across a restart, and
 * the saving is one catalogue fetch.
 */
@Log
@Component
public class CorpusPoller {

    private final @NotNull GitHubCorpus corpus;
    private final boolean enabled;

    /**
     * The fingerprint each document carried the last time this saw the catalogue.
     */
    private final @NotNull ConcurrentMap<String, String> fingerprints = Concurrent.newMap();

    /**
     * The revision the last poll observed, or empty before the first one.
     */
    private volatile @NotNull String revision = "";

    /**
     * Constructs the poller.
     *
     * @param corpus the corpus to watch
     * @param enabled whether the scheduled entry point does anything
     */
    public CorpusPoller(
        @NotNull GitHubCorpus corpus,
        @Value("${skyblock.data.github.poll-enabled:true}") boolean enabled
    ) {
        this.corpus = corpus;
        this.enabled = enabled;
    }

    /**
     * Polls on the configured cadence.
     */
    @Scheduled(fixedDelayString = "${skyblock.data.github.poll-interval-seconds:60}000")
    public void scheduled() {
        if (!this.enabled)
            return;

        try {
            this.poll();
        } catch (Exception exception) {
            // A poll is an observation, so a failed one is not the deployment's problem to escalate
            // - the next one either sees the same revision or a newer one.
            log.warn("corpus poll failed, will retry on the next cadence", exception);
        }
    }

    /**
     * Asks the corpus whether it moved, and reports which documents did.
     *
     * @return the documents whose composed fingerprint changed, empty when the revision stood still
     */
    public @NotNull ConcurrentList<String> poll() {
        ManifestIndex catalogue = this.catalogue().orElse(null);

        if (catalogue == null)
            return Concurrent.newUnmodifiableList();

        ConcurrentList<String> moved = Concurrent.newList();

        for (String name : catalogue.getDocuments().keySet()) {
            String fingerprint = catalogue.fingerprintOf(name).orElse("");
            String seen = this.fingerprints.put(name, fingerprint);

            // An unseen document counts as moved on the first poll only when something was seen
            // before it, so a cold start records the whole corpus without reporting it as a change.
            if (!this.revision.isEmpty() && !fingerprint.equals(seen))
                moved.add(name);
        }

        log.info(
            "corpus moved from '{}' to '{}' - {} document(s) changed",
            this.revision.isEmpty() ? "(none)" : this.revision,
            catalogue.getRevision(),
            moved.size()
        );

        this.revision = catalogue.getRevision();
        return moved.toUnmodifiable();
    }

    /**
     * Asks the origin for a catalogue, which is the only thing here that reaches the network.
     *
     * @return a fresh catalogue, empty when the revision stood still
     */
    protected @NotNull Optional<ManifestIndex> catalogue() {
        return this.corpus.poll();
    }

    /**
     * The revision the last poll observed.
     *
     * @return the revision, empty before the first poll
     */
    public @NotNull String getRevision() {
        return this.revision;
    }

}
