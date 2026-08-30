package dev.sbs.data.poller;

import com.google.gson.Gson;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.store.ManifestIndex;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Covers what the poller reports and what it refuses to do.
 *
 * <p>Its whole job is bookkeeping over fingerprints, so the corpus is stubbed: what matters is that
 * a first sight is not a change, that a companion moving counts as its document moving, and that
 * nothing here rebuilds anything.
 */
class CorpusPollerTest {

    private static final @NotNull Gson GSON = GsonSettings.defaults().create();

    /**
     * A poller answering a scripted sequence of catalogues.
     */
    private static final class ScriptedPoller extends CorpusPoller {

        private final @NotNull ConcurrentList<Optional<ManifestIndex>> script;
        private int index = 0;

        private ScriptedPoller(@NotNull ConcurrentList<Optional<ManifestIndex>> script) {
            super(null, false);
            this.script = script;
        }

        @Override
        protected @NotNull Optional<ManifestIndex> catalogue() {
            return this.index < this.script.size() ? this.script.get(this.index++) : Optional.empty();
        }

    }

    /**
     * The wire shape, so a test catalogue is built the way a real one is parsed.
     */
    private record Catalogue(
        @NotNull String revision,
        @NotNull ConcurrentMap<String, ConcurrentList<ManifestIndex.Layer>> documents
    ) {}

    private static @NotNull ManifestIndex catalogue(@NotNull String revision, @NotNull Map<String, String[]> documents) {
        ConcurrentMap<String, ConcurrentList<ManifestIndex.Layer>> built = Concurrent.newMap();

        documents.forEach((name, shas) -> {
            ConcurrentList<ManifestIndex.Layer> layers = Concurrent.newList();

            for (int index = 0; index < shas.length; index++)
                layers.add(new ManifestIndex.Layer(name + "-" + index + ".json", shas[index]));

            built.put(name, layers);
        });

        return GSON.fromJson(GSON.toJson(new Catalogue(revision, built)), ManifestIndex.class);
    }

    @SafeVarargs
    private static @NotNull ScriptedPoller poller(@NotNull Optional<ManifestIndex>... catalogues) {
        ConcurrentList<Optional<ManifestIndex>> script = Concurrent.newList();

        for (Optional<ManifestIndex> catalogue : catalogues)
            script.add(catalogue);

        return new ScriptedPoller(script);
    }

    @Test
    @DisplayName("a first sight records the corpus without calling it a change")
    void firstPollIsNotAChange() {
        ScriptedPoller poller = poller(Optional.of(catalogue("r1", Map.of("items", new String[] { "a" }))));

        assertThat(poller.poll().isEmpty(), is(true));
        assertThat(poller.getRevision(), equalTo("r1"));
    }

    @Test
    @DisplayName("a revision that stood still reports nothing")
    void unchangedRevisionReportsNothing() {
        ScriptedPoller poller = poller(
            Optional.of(catalogue("r1", Map.of("items", new String[] { "a" }))),
            Optional.empty()
        );

        poller.poll();

        assertThat(poller.poll().isEmpty(), is(true));
        assertThat(poller.getRevision(), equalTo("r1"));
    }

    @Test
    @DisplayName("a document whose hash moved is named, and one that stood still is not")
    void onlyMovedDocumentsAreNamed() {
        ScriptedPoller poller = poller(
            Optional.of(catalogue("r1", Map.of("items", new String[] { "a" }, "regions", new String[] { "z" }))),
            Optional.of(catalogue("r2", Map.of("items", new String[] { "b" }, "regions", new String[] { "z" })))
        );

        poller.poll();

        assertThat(poller.poll(), contains("items"));
        assertThat(poller.getRevision(), equalTo("r2"));
    }

    @Test
    @DisplayName("a second layer moving counts as its document moving")
    void companionLayerCountsAsAChange() {
        ScriptedPoller poller = poller(
            Optional.of(catalogue("r1", Map.of("items", new String[] { "a", "extra1" }))),
            Optional.of(catalogue("r2", Map.of("items", new String[] { "a", "extra2" })))
        );

        poller.poll();

        // The primary file did not move. A rule reading only its hash would report nothing, which
        // is exactly how an override looks like it never happened.
        assertThat(poller.poll(), contains("items"));
    }

    @Test
    @DisplayName("a document that appears is named")
    void newDocumentIsNamed() {
        ScriptedPoller poller = poller(
            Optional.of(catalogue("r1", Map.of("items", new String[] { "a" }))),
            Optional.of(catalogue("r2", Map.of("items", new String[] { "a" }, "regions", new String[] { "z" })))
        );

        poller.poll();

        assertThat(poller.poll(), contains("regions"));
    }

}
