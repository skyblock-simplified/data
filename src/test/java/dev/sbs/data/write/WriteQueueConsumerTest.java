package dev.sbs.data.write;

import api.simplified.skyblock.model.Region;
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
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Covers the drain against a real in-process Hazelcast member, writing straight into a source the
 * way the deployment writes straight into the corpus's writable one.
 *
 * <p>The origin is a recording source rather than GitHub, because what is being tested is what the
 * deployment does with a write - drain it, group it, apply it, and put it back when it fails - and
 * not what a corpus does with one.
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
            this.origin,
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

    private @NotNull Region region(@NotNull String id) {
        return GSON.fromJson(
            String.format("{\"id\":\"%s\",\"name\":\"%s\",\"gameType\":\"SKYBLOCK\",\"mode\":\"HUB\"}", id, id),
            Region.class
        );
    }

    private void enqueue(@NotNull String id) {
        this.queue().add(WriteEnvelope.of(Region.class, this.region(id), WriteRequest.Operation.UPSERT, GSON));
    }

    private @NotNull IQueue<WriteEnvelope> queue() {
        return this.hazelcast.getQueue(WriteQueueConsumer.QUEUE_NAME);
    }

    private @NotNull IMap<UUID, RetryEnvelope> retries() {
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
        assertThat(this.retries().values().iterator().next().getAttempt(), is(1));
    }

    @Test
    @DisplayName("a write out of attempts is dead-lettered rather than retried forever")
    void exhaustedWriteIsDeadLettered() {
        this.origin.failing = true;
        WriteEnvelope envelope = WriteEnvelope.of(Region.class, this.region("HUB"), WriteRequest.Operation.UPSERT, GSON);

        // The cap is two, so an envelope already on its second attempt has one left.
        this.retries().put(
            envelope.getRequestId(),
            RetryEnvelope.forRetry(envelope, 2, Instant.now().minusSeconds(1))
        );

        assertDrains(1);

        assertThat(this.deadLetters().size(), is(1));
        assertThat(this.retries().isEmpty(), is(true));
        assertThat(this.deadLetters().get(envelope.getRequestId()).getTypeName(), equalTo(Region.class.getName()));
    }

    @Test
    @DisplayName("a retry whose wait has not elapsed is left alone")
    void unreadyRetryIsNotDrained() throws Exception {
        WriteEnvelope envelope = WriteEnvelope.of(Region.class, this.region("HUB"), WriteRequest.Operation.UPSERT, GSON);
        this.retries().put(envelope.getRequestId(), RetryEnvelope.forRetry(envelope, 1, Instant.now().plusSeconds(600)));

        assertThat(this.consumer.cycle(), is(0));
        assertThat(this.retries().size(), is(1));
        assertThat(this.origin.applied.isEmpty(), is(true));
    }

    @Test
    @DisplayName("rows of one type drain as one write rather than one each")
    void rowsOfOneTypeAreOneWrite() {
        WriteEnvelope first = WriteEnvelope.of(Region.class, this.region("HUB"), WriteRequest.Operation.UPSERT, GSON);
        WriteEnvelope second = WriteEnvelope.of(Region.class, this.region("BARN"), WriteRequest.Operation.UPSERT, GSON);
        this.retries().put(first.getRequestId(), RetryEnvelope.forRetry(first, 1, Instant.now().minusSeconds(1)));
        this.retries().put(second.getRequestId(), RetryEnvelope.forRetry(second, 1, Instant.now().minusSeconds(1)));

        assertDrains(2);

        // One request carrying both rows, because the origin rewrites a whole document per write.
        assertThat(this.origin.applied.size(), is(1));
        assertThat(this.origin.applied.getFirst().rows().size(), is(2));
    }

    @Test
    @DisplayName("an upsert and a delete of one type stay separate writes")
    void operationsAreNotMerged() {
        WriteEnvelope upsert = WriteEnvelope.of(Region.class, this.region("HUB"), WriteRequest.Operation.UPSERT, GSON);
        WriteEnvelope delete = WriteEnvelope.of(Region.class, this.region("BARN"), WriteRequest.Operation.DELETE, GSON);
        this.retries().put(upsert.getRequestId(), RetryEnvelope.forRetry(upsert, 1, Instant.now().minusSeconds(1)));
        this.retries().put(delete.getRequestId(), RetryEnvelope.forRetry(delete, 1, Instant.now().minusSeconds(1)));

        assertDrains(2);

        assertThat(this.origin.applied.size(), is(2));
        assertThat(
            this.origin.applied.stream().map(WriteRequest::operation).toList(),
            org.hamcrest.Matchers.containsInAnyOrder(WriteRequest.Operation.UPSERT, WriteRequest.Operation.DELETE)
        );
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
     * A source that records what it was asked to write, and can be told to refuse a write.
     *
     * <p>A read fails the case. The consumer applies each write straight to the source and holds no
     * generation, so nothing on the write path has a reason to read.
     */
    private static final class RecordingOrigin implements Source.Writable {

        private final @NotNull CopyOnWriteArrayList<WriteRequest<?>> applied = new CopyOnWriteArrayList<>();
        private boolean failing = false;

        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            throw new AssertionError(String.format("The write path read '%s'", type.getName()));
        }

        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
            if (this.failing)
                throw new JpaException("The origin refused '%s'", request.type().getName());

            this.applied.add(request);
        }

    }

}
