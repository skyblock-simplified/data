package dev.sbs.data.write;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.store.WriteRequest;
import org.jetbrains.annotations.NotNull;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One write instruction as it crosses a process boundary.
 *
 * <p>A {@link WriteRequest} names a live class and holds live rows, which is the right shape for a
 * caller in the same JVM as the source and the wrong shape for a queue. This is the envelope this
 * deployment wraps one in: the type as a name, the row as JSON, and the identity and timestamp the
 * queue needs to trace it. A deployment that does not put writes on a queue writes none of this.
 *
 * <p>Plain {@link Serializable} with only JDK value types, so Hazelcast's default serializer
 * handles it without a registered stream serializer.
 *
 * @see WriteQueueConsumer
 */
public final class WriteEnvelope implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final @NotNull UUID requestId;
    private final @NotNull String typeName;
    private final @NotNull String operation;
    private final @NotNull String payload;
    private final long enqueuedAtEpochMillis;

    private WriteEnvelope(
        @NotNull UUID requestId,
        @NotNull String typeName,
        @NotNull String operation,
        @NotNull String payload,
        long enqueuedAtEpochMillis
    ) {
        this.requestId = requestId;
        this.typeName = typeName;
        this.operation = operation;
        this.payload = payload;
        this.enqueuedAtEpochMillis = enqueuedAtEpochMillis;
    }

    /**
     * Wraps one row so it can travel to whichever process holds the write instruction.
     *
     * @param type the entity class
     * @param row the row to write
     * @param operation what the write does to it
     * @param gson the instance the row is serialized with
     * @param <T> the entity type
     * @return the envelope
     */
    public static <T extends JpaModel> @NotNull WriteEnvelope of(
        @NotNull Class<T> type,
        @NotNull T row,
        @NotNull WriteRequest.Operation operation,
        @NotNull Gson gson
    ) {
        return new WriteEnvelope(
            UUID.randomUUID(),
            type.getName(),
            operation.name(),
            gson.toJson(row, type),
            Instant.now().toEpochMilli()
        );
    }

    /**
     * The identity the queue traces this write by.
     */
    public @NotNull UUID getRequestId() {
        return this.requestId;
    }

    /**
     * The name of the entity class this write applies to.
     */
    public @NotNull String getTypeName() {
        return this.typeName;
    }

    /**
     * When the producer put this on the queue.
     */
    public @NotNull Instant getEnqueuedAt() {
        return Instant.ofEpochMilli(this.enqueuedAtEpochMillis);
    }

    /**
     * What this write does to the row it carries.
     *
     * @return the operation
     * @throws JpaException if the envelope names no operation this library has
     */
    public @NotNull WriteRequest.Operation getOperation() throws JpaException {
        try {
            return WriteRequest.Operation.valueOf(this.operation);
        } catch (IllegalArgumentException exception) {
            throw new JpaException(exception, "'%s' is not a write operation", this.operation);
        }
    }

    /**
     * Resolves the entity class this write names.
     *
     * @return the entity class
     * @throws JpaException if the class is not on this process's classpath, or is not a model
     */
    @SuppressWarnings("unchecked")
    public @NotNull Class<JpaModel> getType() throws JpaException {
        try {
            Class<?> resolved = Class.forName(this.typeName);

            if (!JpaModel.class.isAssignableFrom(resolved))
                throw new JpaException("'%s' is not a model, so nothing can write it", this.typeName);

            return (Class<JpaModel>) resolved;
        } catch (ClassNotFoundException exception) {
            throw new JpaException(exception, "This process holds no class named '%s'", this.typeName);
        }
    }

    /**
     * Rebuilds the row this envelope carries.
     *
     * @param gson the instance the row is parsed with
     * @return the row
     * @throws JpaException if the payload does not bind to the named type
     */
    public @NotNull JpaModel getRow(@NotNull Gson gson) throws JpaException {
        JpaModel row = gson.fromJson(this.payload, this.getType());

        if (row == null)
            throw new JpaException("The envelope for '%s' carries no row", this.typeName);

        return row;
    }

    /**
     * Rebuilds this envelope as a request the library can apply.
     *
     * @param gson the instance the row is parsed with
     * @return the request over this envelope's single row
     * @throws JpaException if the type or the row cannot be rebuilt
     */
    public @NotNull WriteRequest<JpaModel> toRequest(@NotNull Gson gson) throws JpaException {
        Class<JpaModel> type = this.getType();
        List<JpaModel> rows = List.of(this.getRow(gson));

        return this.getOperation() == WriteRequest.Operation.DELETE
            ? WriteRequest.delete(type, rows)
            : WriteRequest.upsert(type, rows);
    }

}
