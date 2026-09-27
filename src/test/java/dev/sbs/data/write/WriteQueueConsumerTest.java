package dev.sbs.data.write;

import api.simplified.skyblock.model.Region;
import api.simplified.skyblock.model.Zone;
import com.google.gson.Gson;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.sbs.api.write.WriteEnvelope;
import dev.sbs.data.DataApi;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Covers the drain against a real in-process Hazelcast member, writing through a {@link JpaConfig}
 * over a recording source the way the deployment writes through the one over the corpus.
 *
 * <p>The origin is a recording source rather than GitHub, because what is being tested is what the
 * deployment does with a write - drain it, group it, apply it, put it back when it fails or is
 * refused, and keep only the newest write of a row - and not what a corpus does with one. It holds
 * the rows it was written and answers the check's reads from them, so a case can ask which write
 * of a row landed last and what became of one naming a row the origin lacks.
 */
@Tag("slow")
class WriteQueueConsumerTest {

    private static final @NotNull Gson GSON = DataApi.getGson();

    private HazelcastInstance hazelcast;
    private RecordingOrigin origin;
    private WriteQueueConsumer consumer;

    @BeforeEach
    void setUp() {
        Config config = new Config();
        config.setClusterName("write-queue-consumer-test-" + UUID.randomUUID());

        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(true);
        network.setPort(0);
        network.setPortCount(100);
        network.setReuseAddress(true);

        JoinConfig join = network.getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);

        this.hazelcast = Hazelcast.newHazelcastInstance(config);
        this.origin = new RecordingOrigin();

        this.consumer = new WriteQueueConsumer(
            this.hazelcast,
            new JpaConfig(models(Region.class, Zone.class, Keyless.class), this.origin),
            new WriteMetrics(new SimpleMeterRegistry()),
            false,
            2,
            1
        );
    }

    @AfterEach
    void tearDown() {
        if (this.hazelcast != null)
            this.hazelcast.shutdown();
    }

    private @NotNull Region region(@NotNull String id, @NotNull String name) {
        return GSON.fromJson(
            String.format("{\"id\":\"%s\",\"name\":\"%s\",\"gameType\":\"SKYBLOCK\",\"mode\":\"HUB\"}", id, name),
            Region.class
        );
    }

    private @NotNull WriteEnvelope envelope(@NotNull String id, @NotNull String name, @NotNull WriteRequest.Operation operation) {
        return WriteEnvelope.of(Region.class, this.region(id, name), operation, GSON);
    }

    /**
     * Binds a zone naming the given region through its plain link.
     *
     * @param id the zone's id
     * @param region the id of the region it sits in
     * @return the zone
     */
    private @NotNull Zone zone(@NotNull String id, @NotNull String region) {
        return GSON.fromJson(String.format("{\"id\":\"%s\",\"name\":\"%s\",\"region\":\"%s\"}", id, id, region), Zone.class);
    }

    /**
     * Wraps a zone upsert the way a producer queues one.
     *
     * @param id the zone's id
     * @param region the id of the region it names
     * @return the envelope
     */
    private @NotNull WriteEnvelope zoneUpsert(@NotNull String id, @NotNull String region) {
        return WriteEnvelope.of(Zone.class, this.zone(id, region), WriteRequest.Operation.UPSERT, GSON);
    }

    /**
     * Lists the models the consumer's config registers.
     *
     * @param types the model classes
     * @return the classes, as a config registers them
     */
    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    private void enqueue(@NotNull String id) {
        this.enqueue(this.envelope(id, id, WriteRequest.Operation.UPSERT));
    }

    private void enqueue(@NotNull WriteEnvelope envelope) {
        this.queue().add(envelope);
    }

    /**
     * Puts a region write on the retry map under its row, the way a failed attempt leaves it.
     *
     * @param id the id of the region the write names
     * @param envelope the write
     * @param attempt the attempt it is tried at next
     * @param readyAt the instant its wait elapses
     */
    private void waiting(@NotNull String id, @NotNull WriteEnvelope envelope, int attempt, @NotNull Instant readyAt) {
        this.retries().put(identity(id), RetryEnvelope.forRetry(envelope, attempt, readyAt));
    }

    /**
     * Makes every waiting write due, standing in for its backoff elapsing.
     */
    private void forceDue() {
        for (Map.Entry<String, RetryEnvelope> entry : this.retries().entrySet()) {
            RetryEnvelope retry = entry.getValue();
            this.retries().put(entry.getKey(), RetryEnvelope.forRetry(retry.getEnvelope(), retry.getAttempt(), Instant.now().minusSeconds(1)));
        }
    }

    /**
     * Reads back the region the origin holds under the given id, if any.
     *
     * @param id the region's id
     * @return the region held, or {@code null} if the origin holds none under that id
     */
    private @Nullable Region held(@NotNull String id) {
        return (Region) this.origin.held(Region.class, id);
    }

    /**
     * Names the retry map's key for the region with the given id - its document and key.
     *
     * @param id the region's id
     * @return the region's row identity
     */
    private static @NotNull String identity(@NotNull String id) {
        return JpaModel.documentOf(Region.class) + "/" + id;
    }

    /**
     * Sets one of an envelope's fields to what a producer on other classes could have sent.
     *
     * <p>{@link WriteEnvelope#of} takes a live class, a typed row and an operation this library
     * has, so it cannot build an envelope that fails to decode; one that does is written field by
     * field.
     *
     * @param envelope the envelope to rewrite in place
     * @param field the name of the envelope field to set
     * @param value the value the field is set to
     * @return the given envelope, rewritten
     */
    private static @NotNull WriteEnvelope forged(@NotNull WriteEnvelope envelope, @NotNull String field, @NotNull String value) {
        try {
            Field declared = WriteEnvelope.class.getDeclaredField(field);
            declared.setAccessible(true);
            declared.set(envelope, value);
            return envelope;
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private @NotNull IQueue<WriteEnvelope> queue() {
        return this.hazelcast.getQueue(WriteQueueConsumer.QUEUE_NAME);
    }

    private @NotNull IMap<String, RetryEnvelope> retries() {
        return this.hazelcast.getMap(WriteQueueConsumer.RETRY_MAP_NAME);
    }

    private @NotNull IMap<UUID, WriteEnvelope> deadLetters() {
        return this.hazelcast.getMap(WriteQueueConsumer.DEAD_LETTER_MAP_NAME);
    }

    @Test
    @DisplayName("a queued write reaches the origin")
    void drainedWriteReachesTheOrigin() throws Exception {
        this.enqueue("HUB");

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.origin.applied.getFirst().type(), equalTo(Region.class));
        assertThat(((Region) this.origin.applied.getFirst().rows().getFirst()).getId(), equalTo("HUB"));
        assertThat(this.held("HUB"), is(notNullValue()));
    }

    @Test
    @DisplayName("an empty queue is a cycle that does nothing")
    void emptyQueueIsANoOp() throws Exception {
        assertThat(this.consumer.cycle(), is(0));
        assertThat(this.origin.applied.isEmpty(), is(true));
    }

    @Test
    @DisplayName("a failed write waits rather than being lost")
    void failedWriteIsHeldForRetry() throws Exception {
        this.origin.failing = true;
        this.enqueue("HUB");

        this.consumer.cycle();

        assertThat(this.retries().size(), is(1));
        assertThat(this.deadLetters().isEmpty(), is(true));
        assertThat(this.retries().get(identity("HUB")).getAttempt(), is(1));
    }

    @Test
    @DisplayName("a write out of attempts is dead-lettered rather than retried forever")
    void exhaustedWriteIsDeadLettered() {
        this.origin.failing = true;
        WriteEnvelope envelope = this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT);

        // The cap is two, so an envelope already on its second attempt has one left.
        this.waiting("HUB", envelope, 2, Instant.now().minusSeconds(1));

        assertDrains(1);

        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.retries().isEmpty(), is(true));
        assertThat(this.deadLetters().get(envelope.getRequestId()).getTypeName(), equalTo(Region.class.getName()));
    }

    @Test
    @DisplayName("a retry whose wait has not elapsed is left alone")
    void unreadyRetryIsNotDrained() throws Exception {
        this.waiting("HUB", this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT), 1, Instant.now().plusSeconds(600));

        assertThat(this.consumer.cycle(), is(0));
        assertThat(this.retries().size(), is(1));
        assertThat(this.origin.applied.isEmpty(), is(true));
    }

    @Test
    @DisplayName("rows of one type drain as one write rather than one each")
    void rowsOfOneTypeAreOneWrite() {
        this.waiting("HUB", this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));
        this.waiting("BARN", this.envelope("BARN", "BARN", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));

        assertDrains(2);

        // One request carrying both rows, because the source rewrites each file a write changes once.
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.origin.applied.getFirst().rows().size(), is(2));
    }

    @Test
    @DisplayName("an upsert and a delete of one type stay separate writes")
    void operationsAreNotMerged() {
        this.waiting("HUB", this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));
        this.waiting("BARN", this.envelope("BARN", "BARN", WriteRequest.Operation.DELETE), 1, Instant.now().minusSeconds(1));

        assertDrains(2);

        assertThat(this.origin.applied.size(), is(2));
        assertThat(
            this.origin.applied.stream().map(WriteRequest::operation).toList(),
            org.hamcrest.Matchers.containsInAnyOrder(WriteRequest.Operation.UPSERT, WriteRequest.Operation.DELETE)
        );
    }

    @Test
    @DisplayName("a fresh write of a row replaces its waiting retry")
    void freshWriteSupersedesItsRowsRetry() throws Exception {
        this.waiting("HUB", this.envelope("HUB", "old", WriteRequest.Operation.UPSERT), 1, Instant.now().plusSeconds(600));
        this.enqueue(this.envelope("HUB", "new", WriteRequest.Operation.UPSERT));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.retries().isEmpty(), is(true));
        assertThat(this.held("HUB").getName(), equalTo("new"));

        // Had the older write been left waiting, this is where its backoff would elapse and it would land.
        this.forceDue();

        assertThat(this.consumer.cycle(), is(0));
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.held("HUB").getName(), equalTo("new"));
    }

    @Test
    @DisplayName("a due retry does not land over a fresh write of its row")
    void dueRetryDoesNotLandOverFreshWrite() throws Exception {
        this.waiting("HUB", this.envelope("HUB", "old", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));
        this.enqueue(this.envelope("HUB", "new", WriteRequest.Operation.UPSERT));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.origin.applied.getFirst().rows().size(), is(1));
        assertThat(this.held("HUB").getName(), equalTo("new"));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a retry of the other operation does not undo a fresh write")
    void retryOfOtherOperationDoesNotUndoFreshWrite() throws Exception {
        this.waiting("HUB", this.envelope("HUB", "old", WriteRequest.Operation.DELETE), 1, Instant.now().minusSeconds(1));
        this.enqueue(this.envelope("HUB", "new", WriteRequest.Operation.UPSERT));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(
            this.origin.applied.stream().map(WriteRequest::operation).toList(),
            not(hasItem(WriteRequest.Operation.DELETE))
        );
        assertThat(this.held("HUB").getName(), equalTo("new"));
    }

    @Test
    @DisplayName("a lost answer is not replayed over a later write")
    void lostAnswerIsNotReplayedOverALaterWrite() throws Exception {
        this.origin.losingAnswers = true;
        this.enqueue(this.envelope("HUB", "old", WriteRequest.Operation.UPSERT));
        this.consumer.cycle();

        // The write landed and its answer did not, so it waits to be sent again like one that failed.
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.held("HUB").getName(), equalTo("old"));
        assertThat(this.retries().size(), is(1));

        this.origin.losingAnswers = false;
        this.enqueue(this.envelope("HUB", "new", WriteRequest.Operation.UPSERT));
        this.consumer.cycle();
        this.forceDue();
        this.consumer.cycle();

        assertThat(this.held("HUB").getName(), equalTo("new"));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a row has one pending write, the newest")
    void onePendingWritePerRow() throws Exception {
        this.origin.failing = true;
        this.enqueue(this.envelope("HUB", "old", WriteRequest.Operation.UPSERT));
        this.consumer.cycle();

        WriteEnvelope newer = this.envelope("HUB", "new", WriteRequest.Operation.UPSERT);
        this.enqueue(newer);
        this.consumer.cycle();

        assertThat(this.retries().size(), is(1));
        assertThat(this.retries().get(identity("HUB")).getEnvelope().getRequestId(), equalTo(newer.getRequestId()));
        assertThat(this.deadLetters().isEmpty(), is(true));
    }

    @Test
    @DisplayName("an envelope naming no operation is dead-lettered and the retries drained beside it still land")
    void malformedEnvelopeIsDeadLetteredAtOnce() throws Exception {
        this.waiting("BARN", this.envelope("BARN", "BARN", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));
        WriteEnvelope malformed = forged(this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT), "operation", "MERGE");
        this.enqueue(malformed);

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.deadLetters().containsKey(malformed.getRequestId()), is(true));
        assertThat(this.held("BARN"), is(notNullValue()));
        assertThat(this.held("HUB"), is(nullValue()));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a retry that no longer decodes is dead-lettered and the retries beside it still land")
    void undecodableRetryIsDeadLettered() throws Exception {
        WriteEnvelope drifted = forged(
            this.envelope("HUB", "HUB", WriteRequest.Operation.UPSERT),
            "typeName",
            "dev.sbs.data.write.RemovedModel"
        );
        this.waiting("HUB", drifted, 1, Instant.now().minusSeconds(1));
        this.waiting("BARN", this.envelope("BARN", "BARN", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.deadLetters().containsKey(drifted.getRequestId()), is(true));
        assertThat(this.held("BARN"), is(notNullValue()));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a row with no key is dead-lettered at once")
    void keylessRowIsDeadLetteredAtOnce() throws Exception {
        WriteEnvelope keyless = WriteEnvelope.of(Keyless.class, new Keyless(), WriteRequest.Operation.UPSERT, GSON);
        this.enqueue(keyless);

        assertThat(this.consumer.cycle(), is(0));
        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.deadLetters().containsKey(keyless.getRequestId()), is(true));
        assertThat(this.origin.applied.isEmpty(), is(true));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a retry whose row no longer carries a key is dead-lettered and its type's other rows still land")
    void keylessRetryIsDeadLettered() throws Exception {
        // Its key was read when it was polled; the id no longer binds, the way a renamed field drifts.
        WriteEnvelope drifted = WriteEnvelope.of(Keyless.class, new Keyless(), WriteRequest.Operation.UPSERT, GSON);
        this.retries().put(JpaModel.documentOf(Keyless.class) + "/K0", RetryEnvelope.forRetry(drifted, 1, Instant.now().minusSeconds(1)));
        this.enqueue(WriteEnvelope.of(Keyless.class, GSON.fromJson("{\"id\":\"K1\"}", Keyless.class), WriteRequest.Operation.UPSERT, GSON));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.deadLetters().containsKey(drifted.getRequestId()), is(true));
        assertThat(this.origin.held(Keyless.class, "K1"), is(notNullValue()));
        assertThat(this.retries().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a refused envelope fails alone and the rest of its group lands")
    void refusedEnvelopeFailsAloneAndItsGroupLands() {
        this.origin.hold(this.region("HUB", "HUB"));
        String stranded = JpaModel.documentOf(Zone.class) + "/STRANDED";
        Instant due = Instant.now().minusSeconds(1);
        this.retries().put(JpaModel.documentOf(Zone.class) + "/VILLAGE", RetryEnvelope.forRetry(this.zoneUpsert("VILLAGE", "HUB"), 1, due));
        this.retries().put(JpaModel.documentOf(Zone.class) + "/FOREST", RetryEnvelope.forRetry(this.zoneUpsert("FOREST", "HUB"), 1, due));
        this.retries().put(stranded, RetryEnvelope.forRetry(this.zoneUpsert("STRANDED", "NOWHERE"), 1, due));

        assertDrains(3);

        // The group's one write is refused whole, so each zone is written again by itself.
        assertThat(this.origin.applied.size(), is(2));
        assertThat(this.origin.applied.stream().allMatch(request -> request.rows().size() == 1), is(true));
        assertThat(this.origin.held(Zone.class, "VILLAGE"), is(notNullValue()));
        assertThat(this.origin.held(Zone.class, "FOREST"), is(notNullValue()));
        assertThat(this.origin.held(Zone.class, "STRANDED"), is(nullValue()));
        assertThat(this.retries().size(), is(1));
        assertThat(this.retries().get(stranded).getAttempt(), is(2));
        assertThat(this.deadLetters().isEmpty(), is(true));
    }

    @Test
    @DisplayName("a delete of a region a zone still names never reaches the origin")
    void danglingDeleteNeverReachesTheOrigin() throws Exception {
        this.origin.hold(this.region("HUB", "HUB"));
        this.origin.hold(this.zone("VILLAGE", "HUB"));
        this.enqueue(this.envelope("HUB", "HUB", WriteRequest.Operation.DELETE));

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.origin.applied.isEmpty(), is(true));
        assertThat(this.held("HUB"), is(notNullValue()));
        assertThat(this.retries().get(identity("HUB")).getAttempt(), is(1));
        assertThat(this.deadLetters().isEmpty(), is(true));
    }

    @Test
    @DisplayName("an upsert naming a row the origin lacks is retried rather than written, and lands once the row does")
    void danglingUpsertIsRetriedNotWritten() throws Exception {
        // The region's own write is due in the same drain, and the fresh zone is written before it.
        this.waiting("CARNIVAL", this.envelope("CARNIVAL", "Carnival", WriteRequest.Operation.UPSERT), 1, Instant.now().minusSeconds(1));
        this.enqueue(this.zoneUpsert("FAIRGROUND", "CARNIVAL"));
        String fairground = JpaModel.documentOf(Zone.class) + "/FAIRGROUND";

        assertThat(this.consumer.cycle(), is(2));
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.origin.applied.getFirst().type(), equalTo(Region.class));
        assertThat(this.origin.held(Zone.class, "FAIRGROUND"), is(nullValue()));
        assertThat(this.retries().get(fairground).getAttempt(), is(1));
        assertThat(this.deadLetters().isEmpty(), is(true));

        this.forceDue();

        assertThat(this.consumer.cycle(), is(1));
        assertThat(this.origin.held(Zone.class, "FAIRGROUND"), is(notNullValue()));
        assertThat(this.retries().isEmpty(), is(true));
        assertThat(this.deadLetters().isEmpty(), is(true));
    }

    /**
     * Runs cycles until the expected number of envelopes has been drained.
     *
     * <p>A cycle takes at most one fresh envelope off the queue and every ready retry, so a case
     * seeding the retry map drains in one - but the queue poll blocks briefly first, and asserting
     * a count rather than a cycle keeps the case about the drain rather than about its timing.
     */
    private void assertDrains(int expected) {
        try {
            assertThat(this.consumer.cycle(), is(expected));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    /**
     * A source that records what it was asked to write and holds the rows it was written, and can
     * be told to refuse a write or to land one and lose its answer.
     *
     * <p>Rows are held by type and then by key, read with {@link JpaModel#keyed} as a document
     * source reads them: an upsert replaces a row whole and a delete removes it. A read answers the
     * rows held for its type, which is what the check before a write reads.
     */
    private static final class RecordingOrigin implements Source.Writable {

        private final @NotNull CopyOnWriteArrayList<WriteRequest<?>> applied = new CopyOnWriteArrayList<>();
        private final @NotNull ConcurrentMap<Class<?>, ConcurrentMap<String, JpaModel>> rows = Concurrent.newMap();
        private boolean failing = false;
        private boolean losingAnswers = false;

        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            ConcurrentMap<String, JpaModel> held = this.rows.get(type);
            return held == null ? Concurrent.newList() : held.values().stream().map(type::cast).collect(Concurrent.toList());
        }

        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
            if (this.failing)
                throw new JpaException("The origin refused '%s'", request.type().getName());

            ConcurrentMap<String, T> keyed = JpaModel.keyed(request.type(), request.rows());
            ConcurrentMap<String, JpaModel> held = this.rows.computeIfAbsent(request.type(), type -> Concurrent.newMap());

            if (request.operation() == WriteRequest.Operation.DELETE)
                keyed.keySet().forEach(held::remove);
            else
                held.putAll(keyed);

            this.applied.add(request);

            if (this.losingAnswers)
                throw new JpaException("The answer to the write of '%s' was lost", request.type().getName());
        }

        /**
         * Reads back the row held for the given type under the given key, if any.
         *
         * @param type the row's type
         * @param key the row's key, as {@link JpaModel#keyed} reads it
         * @return the row held, or {@code null} if none is held under that key
         */
        private @Nullable JpaModel held(@NotNull Class<?> type, @NotNull String key) {
            ConcurrentMap<String, JpaModel> held = this.rows.get(type);
            return held == null ? null : held.get(key);
        }

        /**
         * Holds a row as though another writer had committed it, recording no write.
         *
         * @param row the row to hold
         */
        private void hold(@NotNull JpaModel row) {
            this.rows.computeIfAbsent(row.getClass(), type -> Concurrent.newMap())
                .put(String.valueOf(JpaModel.keyOf(row.getClass()).get(row)), row);
        }

    }

    /**
     * A model whose rows need not carry an id, so a producer can send one that names no row.
     */
    @Table(name = "keyless")
    private static final class Keyless implements JpaModel {

        /**
         * The row's key, which a row may leave unset.
         */
        @Id
        private String id;

    }

}
