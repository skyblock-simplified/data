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

> #### The test hazelcast.xml is loaded by nothing
> `src/test/resources/hazelcast.xml` configures a member - cluster `skyblock-test`, port 5801 with
> auto-increment over 20 ports, every join mechanism off - that no test and no bean reads.
> `WriteQueueConsumerTest` builds its `Config` in code and passes it to
> `Hazelcast.newHazelcastInstance(Config)`, which reads no file; nothing calls the no-argument form,
> the one that would look for a classpath `hazelcast.xml`. `SimplifiedDataApplicationTests` starts a
> context whose one Hazelcast instance is `PersistenceConfig`'s client, built from
> `hazelcast-client.xml`, and none of the Spring Boot 4.0.5 modules on the test classpath carries a
> Hazelcast auto-configuration that would build a member from the file. It is kept; whether a test
> should use it or it should go is open.
>
> - Affected: `src/test/resources/hazelcast.xml`;
>   `src/test/java/dev/sbs/data/write/WriteQueueConsumerTest.java:54-68` - `setUp`
> - Type: **GAP**
> - Status: **OPEN**
