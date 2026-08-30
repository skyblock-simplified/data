package dev.sbs.data.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.sbs.api.write.WriteEnvelope;
import dev.simplified.annotations.Log;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * What the write path publishes for a Prometheus scrape.
 *
 * <p>Every meter here has a producer. A meter for a step that no longer exists reads on a dashboard
 * as a step that is never taken rather than as one that was removed, which is worse than not
 * publishing it - so the commit-mode, per-step and buffer-depth families went with the machinery
 * they measured.
 *
 * <ul>
 *   <li><b>counters</b> - writes received, retried, dead-lettered</li>
 *   <li><b>timers</b> - how long a write takes, and how long one waited from enqueue to applied</li>
 *   <li><b>gauges</b> - how deep the queue, the retry map and the dead-letter map are</li>
 * </ul>
 */
@Log
@Component
public class WriteMetrics {

    /**
     * Writes drained fresh from the queue.
     */
    public static final @NotNull String METER_REQUESTS_RECEIVED = "skyblock.writes.requests.received";

    /**
     * Writes drained from the retry map, tagged by attempt.
     */
    public static final @NotNull String METER_REQUESTS_RETRIED = "skyblock.writes.requests.retried";

    /**
     * Writes moved to the dead-letter map, tagged by entity type.
     */
    public static final @NotNull String METER_DEADLETTER_ADDED = "skyblock.writes.deadletter.added";

    /**
     * How long one write took, tagged by outcome.
     */
    public static final @NotNull String METER_WRITE_DURATION = "skyblock.writes.duration";

    /**
     * How long a write waited between being enqueued and being applied.
     */
    public static final @NotNull String METER_END_TO_END_LATENCY = "skyblock.writes.end_to_end.latency";

    /**
     * How many writes are waiting on the queue.
     */
    public static final @NotNull String METER_PRIMARY_QUEUE_SIZE = "skyblock.writes.primary_queue.size";

    /**
     * How many writes are waiting out a backoff.
     */
    public static final @NotNull String METER_RETRY_IMAP_SIZE = "skyblock.writes.retry_imap.size";

    /**
     * How many writes have run out of attempts.
     */
    public static final @NotNull String METER_DEADLETTER_IMAP_SIZE = "skyblock.writes.deadletter_imap.size";

    private static final @NotNull String TAG_ATTEMPT = "attempt";
    private static final @NotNull String TAG_STATUS = "status";
    private static final @NotNull String TAG_TYPE = "type";

    private static final @NotNull String STATUS_SUCCESS = "success";
    private static final @NotNull String STATUS_FAILURE = "failure";

    private final @NotNull MeterRegistry registry;

    /**
     * Constructs the metrics holder, registering the untagged counters eagerly so the first scrape
     * shows them at zero rather than not at all.
     *
     * @param meterRegistry the registry the container supplies
     */
    public WriteMetrics(@NotNull MeterRegistry meterRegistry) {
        this.registry = meterRegistry;
        this.registry.counter(METER_REQUESTS_RECEIVED);
        this.registry.timer(METER_WRITE_DURATION, TAG_STATUS, STATUS_SUCCESS);
        this.registry.timer(METER_WRITE_DURATION, TAG_STATUS, STATUS_FAILURE);
    }

    /**
     * Records a write drained fresh from the queue.
     */
    public void recordFreshDispatch() {
        this.registry.counter(METER_REQUESTS_RECEIVED).increment();
    }

    /**
     * Records a write drained from the retry map.
     *
     * @param attempt which attempt this is, counting the original dispatch as zero
     */
    public void recordRetryDispatch(int attempt) {
        this.registry.counter(METER_REQUESTS_RETRIED, TAG_ATTEMPT, Integer.toString(attempt)).increment();
    }

    /**
     * Records a write that has run out of attempts.
     *
     * @param typeName the entity class the write named
     */
    public void recordDeadLetter(@NotNull String typeName) {
        this.registry.counter(METER_DEADLETTER_ADDED, TAG_TYPE, typeName).increment();
    }

    /**
     * Records a write that reached the origin.
     *
     * @param started when the write was issued
     */
    public void recordWriteSuccess(@NotNull Instant started) {
        this.record(STATUS_SUCCESS, started);
    }

    /**
     * Records a write that did not.
     *
     * @param started when the write was issued
     */
    public void recordWriteFailure(@NotNull Instant started) {
        this.record(STATUS_FAILURE, started);
    }

    /**
     * Records how long a write waited between being enqueued and being applied.
     *
     * @param enqueuedAt when the producer put it on the queue
     */
    public void recordEndToEnd(@NotNull Instant enqueuedAt) {
        this.registry.timer(METER_END_TO_END_LATENCY).record(Duration.between(enqueuedAt, Instant.now()));
    }

    /**
     * Registers the depth gauges over the queue and the two maps.
     *
     * @param instance the client the queue and its maps live on
     */
    public void registerDepthGauges(@NotNull HazelcastInstance instance) {
        IQueue<WriteEnvelope> queue = instance.getQueue(WriteQueueConsumer.QUEUE_NAME);
        IMap<UUID, RetryEnvelope> retries = instance.getMap(WriteQueueConsumer.RETRY_MAP_NAME);
        IMap<UUID, WriteEnvelope> deadLetters = instance.getMap(WriteQueueConsumer.DEAD_LETTER_MAP_NAME);

        Gauge.builder(METER_PRIMARY_QUEUE_SIZE, queue, q -> (double) q.size())
            .strongReference(true)
            .register(this.registry);

        Gauge.builder(METER_RETRY_IMAP_SIZE, retries, m -> (double) m.size())
            .strongReference(true)
            .register(this.registry);

        Gauge.builder(METER_DEADLETTER_IMAP_SIZE, deadLetters, m -> (double) m.size())
            .strongReference(true)
            .register(this.registry);
    }

    private void record(@NotNull String status, @NotNull Instant started) {
        Timer.builder(METER_WRITE_DURATION)
            .tags(Tags.of(TAG_STATUS, status))
            .register(this.registry)
            .record(Duration.between(started, Instant.now()));
    }

}
