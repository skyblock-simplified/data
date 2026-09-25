package dev.sbs.data;

import com.google.gson.Gson;
import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.gson.GsonSettings;
import org.jetbrains.annotations.NotNull;

/**
 * Service locator for this deployment's {@link Gson}.
 * <p>
 * {@link GsonSettings#defaults()} walks the {@code ServiceLoader} SPI and picks up a contributor
 * from every jar on the classpath, so nothing here registers an adapter by hand - including the
 * exclusion that keeps a resolved link out of a row on its way onto the write queue.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataApi {

    @Getter private static final @NotNull GsonSettings gsonSettings = GsonSettings.defaults();
    @Getter private static final @NotNull Gson gson = gsonSettings.create();

}
