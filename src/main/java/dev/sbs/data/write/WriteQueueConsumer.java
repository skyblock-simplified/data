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
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import dev.simplified.persistence.exception.JpaException;
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
 * is one question - did this write reach the origin - and the answer is the config
 * {@link SkyBlockData#writing(GitHubCorpus)} returns, whose {@link JpaConfig#write} checks the
 * write, applies it and throws when it is refused or fails. No session stands between them, so
 * this service holds no generation for a write to rebuild. Besides the layers it rewrites, a write
 * reads before it commits the documents a plain link - a {@link Linked} field that is neither a
 * list nor optional - can dangle into: for an upsert, those its rows' plain links name, and for a
 * delete, those of every type declaring a plain link into the deleted type. It is refused when it
 * would leave such a link naming no row.
 *
 * <p>An envelope states one row's whole final state - an upsert replaces the row and a delete
 * removes it - so of several writes to one row only the newest matters. Writes are therefore
 * tracked by row, named by its document and key through the {@link JpaModel} accessors the source
 * keys rows with: the retry map holds at most one waiting write per row, and a fresh envelope drops
 * its row's waiting write, due or not. One drain takes the queue in order, so the waiting write is
 * always the older one, and a write that failed, or that landed and lost its answer, is never
 * replayed over a later queued write of its row.
 *
 * <p>A cycle takes one fresh envelope and every due retry, groups them by type and operation, and
 * issues one write per group. Grouping is worth doing because a document source rewrites each file
 * a write changes once, so N rows of one type in one request are one commit per changed file rather
 * than N. It is not a correctness concern - a write of one row is a legal write - so a group of
 * more than one envelope that fails, which one refused or failing row is enough for, is written
 * again one envelope at a time.
 *
 * <p>A failure puts each envelope that failed on its own back under its row with its attempt
 * counter raised and its delay doubled, until the cap, after which it is dead-lettered for an
 * operator, under its request id, rather than retried forever. A refused write waits like any
 * other failure, since the write that lets it pass - the upsert of the row it names, or the delete
 * or re-point of a row naming a row it deletes - can be due in the same drain and land after it,
 * and then it passes at its next attempt. An envelope that does not decode - a type this process
 * does not hold, an operation it does not know or a row that does not bind - is dead-lettered as
 * soon as it is drained, as is a row that carries no key, since no attempt would write either and
 * a keyless row would fail every row written beside it.
 */
@Log
@Component
public class WriteQueueConsumer {

    /**
     * The queue producers put fresh writes on.
     */
    public static final @NotNull String QUEUE_NAME = "skyblock.writes";

    /**
     * The map a write waits in between attempts, keyed by the row it writes.
     */
    public static final @NotNull String RETRY_MAP_NAME = "skyblock.writes.retry";

    /**
     * The map a write lands in once it has run out of attempts.
     */
    public static final @NotNull String DEAD_LETTER_MAP_NAME = "skyblock.writes.deadletter";

    private static final long POLL_TIMEOUT_MILLIS = 500;

    private final @NotNull HazelcastInstance hazelcast;
    private final @NotNull JpaConfig config;
    private final @NotNull WriteMetrics metrics;
    private final @NotNull Gson gson = DataApi.getGson();
    private final boolean enabled;
    private final int maxAttempts;
    private final @NotNull Duration initialDelay;

    private volatile Thread drain;

    /**
     * Constructs the consumer over the config {@link SkyBlockData#writing(GitHubCorpus)} returns
     * for the given corpus.
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
     * Constructs the consumer over the given config.
     *
     * @param hazelcast the client the queue and its maps live on
     * @param config the models and the source every write is checked against and applied through
     * @param metrics the deployment's write observability
     * @param enabled whether the drain thread starts
     * @param maxAttempts how many retries a write gets before it is dead-lettered
     * @param initialDelayMinutes the delay before the first retry, doubling thereafter
     */
    WriteQueueConsumer(
        @NotNull HazelcastInstance hazelcast,
        @NotNull JpaConfig config,
        @NotNull WriteMetrics metrics,
        boolean enabled,
        int maxAttempts,
        long initialDelayMinutes
    ) {
        this.hazelcast = hazelcast;
        this.config = config;
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
     * Drains what is waiting and applies it, one write per type and operation, and one per envelope
     * of a group of several that fails.
     *
     * <p>Every envelope is decoded as it is drained, so the write is built from rows that already
     * bound and carry a key. The fresh envelope is named by its row there, and drops whatever write
     * of that row is waiting on the retry map before the due retries are taken, so no drain carries
     * a row twice.
     *
     * @return the number of envelopes applied
     * @throws InterruptedException if the drain is interrupted while waiting on the queue
     */
    public int cycle() throws InterruptedException {
        ConcurrentMap<String, WriteEnvelope> drained = Concurrent.newLinkedMap();
        ConcurrentMap<String, Integer> attempts = Concurrent.newMap();
        ConcurrentMap<String, WriteRequest<JpaModel>> decoded = Concurrent.newMap();

        WriteEnvelope fresh = this.queue().poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        if (fresh != null) {
            this.metrics.recordFreshDispatch();
            String row = null;

            try {
                WriteRequest<JpaModel> request = fresh.toRequest(this.gson);
                Object key = JpaModel.keyOf(request.type()).get(request.rows().getFirst());

                if (key == null)
                    throw new JpaException("A row of '%s' carries no id, so nothing can name it", fresh.getTypeName());

                row = JpaModel.documentOf(request.type()) + "/" + key;
                decoded.put(row, request);
            } catch (RuntimeException exception) {
                this.deadLetter(fresh);
                log.error(
                    "write '{}' of '{}' dead-lettered, it does not decode to a row it can name",
                    fresh.getRequestId(), fresh.getTypeName(), exception
                );
            }

            if (row != null) {
                // The queue is drained in order, so a write of this row still waiting is older than
                // this one, which states the row's whole final state.
                RetryEnvelope superseded = this.retries().remove(row);

                if (superseded != null) {
                    log.info(
                        "write '{}' of '{}' supersedes the waiting write '{}'",
                        fresh.getRequestId(), row, superseded.getEnvelope().getRequestId()
                    );
                }

                drained.put(row, fresh);
                attempts.put(row, 0);
            }
        }

        long now = Instant.now().toEpochMilli();

        for (Map.Entry<String, RetryEnvelope> waiting : this.retries().entrySet()) {
            RetryEnvelope retry = waiting.getValue();

            // Removing before dispatching is what stops a second consumer taking the same write.
            if (!retry.isReady(now) || !this.retries().remove(waiting.getKey(), retry))
                continue;

            this.metrics.recordRetryDispatch(retry.getAttempt());

            try {
                // Its map key names its row, but the source keys rows by their id, so a row whose id
                // no longer binds fails the whole group it would be written in.
                WriteRequest<JpaModel> request = retry.getEnvelope().toRequest(this.gson);

                if (JpaModel.keyOf(request.type()).get(request.rows().getFirst()) == null)
                    throw new JpaException("A row of '%s' no longer carries an id", retry.getEnvelope().getTypeName());

                decoded.put(waiting.getKey(), request);
            } catch (RuntimeException exception) {
                // It decoded when it was polled, so a class it names has changed under it since.
                this.deadLetter(retry.getEnvelope());
                log.error(
                    "write '{}' of '{}' dead-lettered, it no longer decodes to a row it can name",
                    retry.getEnvelope().getRequestId(), retry.getEnvelope().getTypeName(), exception
                );
                continue;
            }

            drained.put(waiting.getKey(), retry.getEnvelope());
            attempts.put(waiting.getKey(), retry.getAttempt());
        }

        if (drained.isEmpty())
            return 0;

        this.apply(drained, attempts, decoded);
        return drained.size();
    }

    /**
     * Groups the drained envelopes and writes each group.
     *
     * <p>Nothing here decodes, so the only thing that can fail is a write. A group is checked and
     * applied as one request, which one refused or failing row fails whole, so a group of more than
     * one envelope that fails is written again one envelope at a time, and only the envelopes that
     * fail alone go back under their rows.
     *
     * @param drained the envelopes taken this cycle, by row, in the order they were taken
     * @param attempts the attempt each envelope was taken at, by row
     * @param decoded the single-row request each envelope decoded to, by row
     */
    private void apply(
        @NotNull ConcurrentMap<String, WriteEnvelope> drained,
        @NotNull ConcurrentMap<String, Integer> attempts,
        @NotNull ConcurrentMap<String, WriteRequest<JpaModel>> decoded
    ) {
        ConcurrentMap<String, ConcurrentList<String>> grouped = Concurrent.newLinkedMap();

        for (Map.Entry<String, WriteEnvelope> entry : drained) {
            WriteRequest<JpaModel> request = decoded.get(entry.getKey());
            String group = request.type().getName() + "/" + request.operation();
            grouped.computeIfAbsent(group, unused -> Concurrent.newList()).add(entry.getKey());
        }

        for (ConcurrentList<String> rows : grouped.values()) {
            if (this.write(this.request(decoded, rows), rows, drained))
                continue;

            for (String row : rows) {
                if (rows.size() == 1 || !this.write(decoded.get(row), Concurrent.newList(row), drained))
                    this.reschedule(row, drained.get(row), attempts.get(row) + 1);
            }
        }
    }

    /**
     * Checks and applies one request through the config, and records how it went.
     *
     * @param request the write, over rows of one type and operation
     * @param rows the rows it carries, by row identity
     * @param drained the envelopes taken this cycle, by row
     * @return whether the write landed
     */
    private boolean write(
        @NotNull WriteRequest<JpaModel> request,
        @NotNull ConcurrentList<String> rows,
        @NotNull ConcurrentMap<String, WriteEnvelope> drained
    ) {
        Instant started = Instant.now();

        try {
            this.config.write(request);
            this.metrics.recordWriteSuccess(started);
            rows.forEach(row -> this.metrics.recordEndToEnd(drained.get(row).getEnqueuedAt()));
            return true;
        } catch (Exception exception) {
            this.metrics.recordWriteFailure(started);
            log.warn(
                "{} of '{}' failed for {} envelope(s)",
                request.operation(), rows.size() == 1 ? rows.getFirst() : request.type().getName(), rows.size(), exception
            );
            return false;
        }
    }

    /**
     * Builds one request over every row of one group.
     *
     * @param decoded the single-row request each envelope decoded to, by row
     * @param rows the rows of the group, all of one type and operation
     * @return the request over them
     */
    private @NotNull WriteRequest<JpaModel> request(
        @NotNull ConcurrentMap<String, WriteRequest<JpaModel>> decoded,
        @NotNull ConcurrentList<String> rows
    ) {
        WriteRequest<JpaModel> first = decoded.get(rows.getFirst());
        ConcurrentList<JpaModel> gathered = Concurrent.newList();
        rows.forEach(row -> gathered.addAll(decoded.get(row).rows()));

        return first.operation() == WriteRequest.Operation.DELETE
            ? WriteRequest.delete(first.type(), gathered)
            : WriteRequest.upsert(first.type(), gathered);
    }

    /**
     * Puts a failed write back to wait under its row, or dead-letters it once it is out of attempts.
     *
     * @param row the row the write names, as its document and key
     * @param envelope the write
     * @param attempt the attempt it would be tried at next
     */
    private void reschedule(@NotNull String row, @NotNull WriteEnvelope envelope, int attempt) {
        if (attempt > this.maxAttempts) {
            this.deadLetter(envelope);
            log.error(
                "write '{}' of '{}' dead-lettered after {} attempt(s)",
                envelope.getRequestId(), envelope.getTypeName(), this.maxAttempts
            );
            return;
        }

        this.retries().put(
            row,
            RetryEnvelope.forRetry(
                envelope,
                attempt,
                RetryEnvelope.computeReadyAt(Instant.now(), attempt, this.initialDelay)
            )
        );
    }

    /**
     * Keeps a write for an operator under its request id, rather than trying it again.
     *
     * @param envelope the write
     */
    private void deadLetter(@NotNull WriteEnvelope envelope) {
        this.deadLetters().put(envelope.getRequestId(), envelope);
        this.metrics.recordDeadLetter(envelope.getTypeName());
    }

    private @NotNull IQueue<WriteEnvelope> queue() {
        return this.hazelcast.getQueue(QUEUE_NAME);
    }

    private @NotNull IMap<String, RetryEnvelope> retries() {
        return this.hazelcast.getMap(RETRY_MAP_NAME);
    }

    private @NotNull IMap<UUID, WriteEnvelope> deadLetters() {
        return this.hazelcast.getMap(DEAD_LETTER_MAP_NAME);
    }

}
