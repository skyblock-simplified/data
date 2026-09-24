# Known open

Open items in `data` on `feat/indexing`. Each stays here until it is closed or accepted.

> #### The scrape endpoint is probably refused, because the permit-all security config never registers
> `SimplifiedData` scans `dev.sbs.data` and `dev.sbs.serverapi`, but `spring-framework`, pinned at
> `6c1497b`, declares its configuration under `dev.simplified.serverapi` and ships no
> auto-configuration imports that would register it without a scan. So
> `dev.simplified.serverapi.security.PermitAllSecurityConfig` is never registered. The Spring Boot
> security starter reaches the classpath as an `api` dependency of `spring-framework`, and with no
> `SecurityFilterChain` bean of the application's own Spring Boot applies its default chain, which
> requires authentication on every request - `/actuator/prometheus` included - so the scrape the
> `infra/prometheus` stack makes would be refused. This is read from the code and has not been
> checked against a running container. `SimplifiedData`'s javadoc and `application.properties` both
> say the scan registers the framework's configuration.
>
> - Affected: `src/main/java/dev/sbs/data/SimplifiedData.java:18` - `scanBasePackages`, javadoc at
>   `:14-16`; `src/main/resources/application.properties:9-12`
> - Type: **RISK**
> - Status: **OPEN**
