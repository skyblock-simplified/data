package dev.sbs.data.write;

import api.simplified.skyblock.model.Region;
import com.google.gson.Gson;
import dev.sbs.data.DataApi;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.store.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers what survives a trip through the queue.
 *
 * <p>A write request names a live class and holds live rows, which a queue cannot carry, so this is
 * the envelope wrapped around one. What matters is that the far side rebuilds the same instruction:
 * the same type, the same operation and a row equal to the one that was sent.
 */
class WriteEnvelopeTest {

    private static final @NotNull Gson GSON = DataApi.getGson();

    private static @NotNull Region region(@NotNull String id, @NotNull String name) {
        Region region = GSON.fromJson(
            String.format("{\"id\":\"%s\",\"name\":\"%s\",\"gameType\":\"SKYBLOCK\",\"mode\":\"HUB\"}", id, name),
            Region.class
        );

        assertThat(region, is(org.hamcrest.Matchers.notNullValue()));
        return region;
    }

    @Test
    @DisplayName("an envelope rebuilds the request it was made from")
    void roundTripsARequest() {
        Region hub = region("HUB", "Hub");
        WriteEnvelope envelope = WriteEnvelope.of(Region.class, hub, WriteRequest.Operation.UPSERT, GSON);

        WriteRequest<?> rebuilt = envelope.toRequest(GSON);

        assertThat(rebuilt.type(), equalTo(Region.class));
        assertThat(rebuilt.operation(), equalTo(WriteRequest.Operation.UPSERT));
        assertThat(rebuilt.rows().size(), is(1));
        assertThat(((Region) rebuilt.rows().getFirst()).getName(), equalTo("Hub"));
    }

    @Test
    @DisplayName("a delete crosses as a delete")
    void carriesTheOperation() {
        WriteEnvelope envelope = WriteEnvelope.of(
            Region.class,
            region("HUB", "Hub"),
            WriteRequest.Operation.DELETE,
            GSON
        );

        assertThat(envelope.getOperation(), equalTo(WriteRequest.Operation.DELETE));
        assertThat(envelope.toRequest(GSON).operation(), equalTo(WriteRequest.Operation.DELETE));
    }

    @Test
    @DisplayName("every envelope carries its own identity")
    void identityIsPerEnvelope() {
        Region hub = region("HUB", "Hub");

        assertThat(
            WriteEnvelope.of(Region.class, hub, WriteRequest.Operation.UPSERT, GSON).getRequestId()
                .equals(WriteEnvelope.of(Region.class, hub, WriteRequest.Operation.UPSERT, GSON).getRequestId()),
            is(false)
        );
    }

    @Test
    @DisplayName("a type this process does not have is a refusal naming it")
    void unknownTypeIsNamed() {
        WriteEnvelope envelope = WriteEnvelope.of(
            Region.class,
            region("HUB", "Hub"),
            WriteRequest.Operation.UPSERT,
            GSON
        );

        // A queue outlives a deployment, so an envelope naming a type this process was not built
        // with has to say which one rather than failing as a cast.
        WriteEnvelope renamed = GSON.fromJson(
            GSON.toJson(envelope).replace(Region.class.getName(), "api.simplified.skyblock.model.Nonexistent"),
            WriteEnvelope.class
        );

        JpaException thrown = assertThrows(JpaException.class, renamed::getType);
        assertThat(thrown.getMessage().contains("Nonexistent"), is(true));
    }

}
