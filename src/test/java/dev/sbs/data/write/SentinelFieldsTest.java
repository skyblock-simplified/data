package dev.sbs.data.write;

import api.simplified.skyblock.model.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Guard over the {@link Event} field names {@link SmokeWriteSentinel} reaches for by reflection.
 *
 * <p>
 * The sentinel builds its entity through {@link Class#getDeclaredField(String)}, so a column renamed
 * on {@link Event} still compiles here and only surfaces when the {@code smoke} profile runs against
 * a live cluster - as a {@code NoSuchFieldException} wrapped in a startup log line nobody is
 * watching. This test turns that into a red build instead.
 */
class SentinelFieldsTest {

    @Test
    @DisplayName("the id field the smoke sentinel sets resolves on Event")
    void idFieldResolves() {
        assertDoesNotThrow(() -> declaredField("id"));
    }

    @Test
    @DisplayName("the name field the smoke sentinel sets resolves on Event")
    void nameFieldResolves() {
        assertDoesNotThrow(() -> declaredField("name"));
    }

    @Test
    @DisplayName("the description field the smoke sentinel sets resolves on Event")
    void descriptionFieldResolves() {
        assertDoesNotThrow(() -> declaredField("description"));
    }

    private static Field declaredField(String fieldName) throws NoSuchFieldException {
        return Event.class.getDeclaredField(fieldName);
    }

}
