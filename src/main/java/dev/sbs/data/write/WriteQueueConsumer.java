package dev.sbs.data.write;

import api.simplified.github.GitHubCorpus;
import api.simplified.skyblock.SkyBlockData;
import com.google.gson.Gson;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.sbs.api.write.WriteEnvelope;
import dev.sbs.data.DataApi;
import dev.simplified.annotations.Log;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PreDestroy;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Drains the write queue and applies what it finds to the corpus.
 *
 * <p>The queue, the backoff and the dead-letter map are this deployment's. What the library answers
 * is one question - did this write reach the origin - and the answer is the corpus's writable
 * source, {@link SkyBlockData#writing(GitHubCorpus)}, whose {@link Source.Writable#write} applies
 * the write and throws when it fails. No session stands between them: this service reads nothing
 * it does not write, so it holds no generation for a write to rebuild.
 *
 * <p>A cycle drains what is waiting, groups it by type and operation, and issues one write per
 * group. Grouping is worth doing because a document origin rewrites a whole file per write, so N
 * rows of one type in one request is one commit rather than N. It is not a correctness concern - a
 * write of one row is a legal write.
 *
 * <p>A failure puts the envelope back with its attempt counter raised and its delay doubled, until
 * the cap, after which it is dead-lettered for an operator rather than retried forever.
 */
@Log
@Component
public class WriteQueueConsumer {

    /**
     * The queue producers put fresh writes on.
     */
    public static final @NotNull String QUEUE_NAME = "skyblock.writes";

    /**
     * The map a write waits in between attempts.
     */
    public static final @NotNull String RETRY_MAP_NAME = "skyblock.writes.retry";

    /**
     * The map a write lands in once it has run out of attempts.
     */
    public static final @NotNull String DEAD_LETTER_MAP_NAME = "skyblock.writes.deadletter";

    private static final long POLL_TIMEOUT_MILLIS = 500;

    private final @NotNull HazelcastInstance hazelcast;
    private final @NotNull Source.Writable source;
    private final @NotNull WriteMetrics metrics;
    private final @NotNull Gson gson = DataApi.getGson();
    private final boolean enabled;
    private final int maxAttempts;
    private final @NotNull Duration initialDelay;

    private volatile Thread drain;

    /**
     * Constructs the consumer over the writable source of the given corpus.
     *
     * @param hazelcast the client the queue and its maps live on
     * @param corpus the corpus the writes are applied to, named with the token that makes it writable
     * @param metrics the deployment's write observability
     * @param enabled whether the drain thread starts
     * @param maxAttempts how many retries a write gets before it is dead-lettered
     * @param initialDelayMinutes the delay before the first retry, doubling thereafter
     */
    @Autowired
    public WriteQueueConsumer(
        @NotNull HazelcastInstance hazelcast,
        @NotNull GitHubCorpus corpus,
        @NotNull WriteMetrics metrics,
        @Value("${skyblock.data.github.write-consumer-enabled:true}") boolean enabled,
        @Value("${skyblock.data.github.write-retry-max-attempts:5}") int maxAttempts,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long initialDelayMinutes
    ) {
        this(hazelcast, SkyBlockData.writing(corpus), metrics, enabled, maxAttempts, initialDelayMinutes);
    }

    /**
     * Constructs the consumer over the given writable source.
     *
     * @param hazelcast the client the queue and its maps live on
     * @param source the source the writes are applied through
     * @param metrics the deployment's write observability
     * @param enabled whether the drain thread starts
     * @param maxAttempts how many retries a write gets before it is dead-lettered
     * @param initialDelayMinutes the delay before the first retry, doubling thereafter
     */
    WriteQueueConsumer(
        @NotNull HazelcastInstance hazelcast,
        @NotNull Source.Writable source,
        @NotNull WriteMetrics metrics,
        boolean enabled,
        int maxAttempts,
        long initialDelayMinutes
    ) {
        this.hazelcast = hazelcast;
        this.source = source;
        this.metrics = metrics;
        this.enabled = enabled;
        this.maxAttempts = maxAttempts;
        this.initialDelay = Duration.ofMinutes(initialDelayMinutes);
    }

    /**
     * Starts the drain once the container is up.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!this.enabled) {
            log.info("write consumer disabled, nothing will drain '{}'", QUEUE_NAME);
            return;
        }

        this.metrics.registerDepthGauges(this.hazelcast);

        Thread thread = new Thread(this::run, "skyblock-write-drain");
        thread.setDaemon(true);
        this.drain = thread;
        thread.start();
        log.info("write consumer draining '{}'", QUEUE_NAME);
    }

    @PreDestroy
    void stop() {
        Thread thread = this.drain;

        if (thread != null) {
            thread.interrupt();

            try {
                thread.join(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                this.cycle();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception exception) {
                // One bad cycle must not stop the drain, or a single malformed envelope takes the
                // whole write path down until someone restarts the service.
                log.error("write drain cycle failed", exception);
            }
        }
    }

    /**
     * Drains what is waiting and applies it, one write per type and operation.
     *
     * @return the number of envelopes applied
     * @throws InterruptedException if the drain is interrupted while waiting on the queue
     */
    public int cycle() throws InterruptedException {
        ConcurrentMap<UUID, WriteEnvelope> drained = Concurrent.newLinkedMap();
        ConcurrentMap<UUID, Integer> attempts = Concurrent.newMap();

        WriteEnvelope fresh = this.queue().poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        if (fresh != null) {
            drained.put(fresh.getRequestId(), fresh);
            attempts.put(fresh.getRequestId(), 0);
            this.metrics.recordFreshDispatch();
        }

        long now = Instant.now().toEpochMilli();

        for (Map.Entry<UUID, RetryEnvelope> waiting : this.retries().entrySet()) {
            if (!waiting.getValue().isReady(now))
                continue;

            // Removing before dispatching is what stops a second consumer taking the same write.
            if (this.retries().remove(waiting.getKey(), waiting.getValue())) {
                drained.put(waiting.getKey(), waiting.getValue().getEnvelope());
                attempts.put(waiting.getKey(), waiting.getValue().getAttempt());
                this.metrics.recordRetryDispatch(waiting.getValue().getAttempt());
            }
        }

        if (drained.isEmpty())
            return 0;

        this.apply(drained, attempts);
        return drained.size();
    }

    /**
     * Groups the drained envelopes and writes each group.
     */
    private void apply(
        @NotNull ConcurrentMap<UUID, WriteEnvelope> drained,
        @NotNull ConcurrentMap<UUID, Integer> attempts
    ) {
        ConcurrentMap<String, ConcurrentList<UUID>> grouped = Concurrent.newLinkedMap();

        for (Map.Entry<UUID, WriteEnvelope> entry : drained) {
            String group = entry.getValue().getTypeName() + "/" + entry.getValue().getOperation();
            grouped.computeIfAbsent(group, unused -> Concurrent.newList()).add(entry.getKey());
        }

        for (Map.Entry<String, ConcurrentList<UUID>> group : grouped) {
            ConcurrentList<UUID> ids = group.getValue();
            Instant started = Instant.now();

            try {
                this.source.write(this.request(drained, ids));
                this.metrics.recordWriteSuccess(started);
                ids.forEach(id -> this.metrics.recordEndToEnd(drained.get(id).getEnqueuedAt()));
            } catch (Exception exception) {
                this.metrics.recordWriteFailure(started);
                log.warn("write of '{}' failed for {} envelope(s)", group.getKey(), ids.size(), exception);
                ids.forEach(id -> this.reschedule(id, drained.get(id), attempts.get(id) + 1));
            }
        }
    }

    /**
     * Builds one request over every row of one group.
     */
    private @NotNull WriteRequest<JpaModel> request(
        @NotNull ConcurrentMap<UUID, WriteEnvelope> drained,
        @NotNull ConcurrentList<UUID> ids
    ) {
        WriteEnvelope first = drained.get(ids.getFirst());
        Class<JpaModel> type = first.getType();
        ConcurrentList<JpaModel> rows = Concurrent.newList();
        ids.forEach(id -> rows.add(drained.get(id).getRow(this.gson)));

        return first.getOperation() == WriteRequest.Operation.DELETE
            ? WriteRequest.delete(type, rows)
            : WriteRequest.upsert(type, rows);
    }

    /**
     * Puts a failed write back to wait, or dead-letters it once it is out of attempts.
     */
    private void reschedule(@NotNull UUID id, @NotNull WriteEnvelope envelope, int attempt) {
        if (attempt > this.maxAttempts) {
            this.deadLetters().put(id, envelope);
            this.metrics.recordDeadLetter(envelope.getTypeName());
            log.error(
                "write '{}' of '{}' dead-lettered after {} attempt(s)",
                id, envelope.getTypeName(), this.maxAttempts
            );
            return;
        }

        this.retries().put(
            id,
            RetryEnvelope.forRetry(
                envelope,
                attempt,
                RetryEnvelope.computeReadyAt(Instant.now(), attempt, this.initialDelay)
            )
        );
    }

    private @NotNull IQueue<WriteEnvelope> queue() {
        return this.hazelcast.getQueue(QUEUE_NAME);
    }

    private @NotNull IMap<UUID, RetryEnvelope> retries() {
        return this.hazelcast.getMap(RETRY_MAP_NAME);
    }

    private @NotNull IMap<UUID, WriteEnvelope> deadLetters() {
        return this.hazelcast.getMap(DEAD_LETTER_MAP_NAME);
    }

}
