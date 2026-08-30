package dev.sbs.data.write;

import dev.sbs.api.write.WriteEnvelope;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * A write waiting out its backoff before it is tried again.
 *
 * <p>Held in the {@code skyblock.writes.retry} map rather than in the consumer's own memory, so a
 * restart of this service picks the in-flight retries back up rather than dropping them: the next
 * process's first scan finds every entry whose ready instant has already elapsed.
 *
 * <p>Retry and escalation are this deployment's, not the library's. What the library answers is
 * whether one write succeeded.
 *
 * @see WriteQueueConsumer
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RetryEnvelope implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    /**
     * The write this envelope is holding back.
     */
    private final @NotNull WriteEnvelope envelope;

    /**
     * Which attempt this represents. The original dispatch is attempt zero, so the first retry is
     * one, and the counter is what the dead-letter cap is read against.
     */
    private final int attempt;

    /**
     * When this becomes eligible again, as epoch millis - a {@code long} rather than an
     * {@link Instant} so the serialized form carries no JDK version drift across the cluster.
     */
    private final long readyAtEpochMillis;

    /**
     * Holds a write back until the given instant.
     *
     * @param envelope the write to retry
     * @param attempt the attempt this represents
     * @param readyAt when it becomes eligible
     * @return the envelope
     */
    public static @NotNull RetryEnvelope forRetry(
        @NotNull WriteEnvelope envelope,
        int attempt,
        @NotNull Instant readyAt
    ) {
        return new RetryEnvelope(envelope, attempt, readyAt.toEpochMilli());
    }

    /**
     * When an attempt becomes eligible, doubling the delay each time.
     *
     * <p>The first retry is ready at {@code now + initialDelay}; the Nth at
     * {@code now + initialDelay * 2^(N-1)}.
     *
     * @param now the current instant
     * @param attempt the attempt counter, one for the first retry
     * @param initialDelay the delay before the first retry
     * @return the instant the attempt becomes eligible
     */
    public static @NotNull Instant computeReadyAt(
        @NotNull Instant now,
        int attempt,
        @NotNull Duration initialDelay
    ) {
        return now.plus(initialDelay.multipliedBy(1L << (attempt - 1)));
    }

    /**
     * Whether this envelope's wait has elapsed.
     *
     * @param nowEpochMillis the current instant, as epoch millis
     * @return {@code true} when it may be dispatched again
     */
    public boolean isReady(long nowEpochMillis) {
        return this.readyAtEpochMillis <= nowEpochMillis;
    }

}
