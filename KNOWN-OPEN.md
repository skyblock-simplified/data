# Known open

Open items in `data`. Each stays here until it is closed or accepted.

> #### The scrape endpoint is probably refused, because the permit-all security config never registers
> `SimplifiedData` scans `dev.sbs.data` and `dev.sbs.serverapi`, but `spring-framework`, pinned at
> `fe32583`, declares its configuration under `dev.simplified.serverapi` and ships no
> auto-configuration imports that would register it without a scan. So
> `dev.simplified.serverapi.security.PermitAllSecurityConfig` is never registered. The Spring Boot
> security starter reaches the classpath as an `api` dependency of `spring-framework`, and with no
> `SecurityFilterChain` bean of the application's own Spring Boot applies its default chain, which
> permits the health endpoint and requires authentication on every other request -
> `/actuator/prometheus` included - so the scrape the `infra/prometheus` stack makes would be
> refused. This is read from the code and has not been checked against a running container.
> `SimplifiedData`'s javadoc and `application.properties` both say the scan registers the
> framework's configuration, and the javadoc also says the only endpoint served is
> `/actuator/prometheus`, where the container serves Actuator's other exposed endpoints, Spring
> Boot's `/error`, and the `/login` and `/logout` of the default chain as well.
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
>   `src/test/java/dev/sbs/data/write/WriteQueueConsumerTest.java:67-94` - `setUp`
> - Type: **GAP**
> - Status: **OPEN**

> #### A retry left under a request-id key fails the cycle that takes it
> The retry map is keyed by the row a write names. An entry an earlier consumer left in
> `skyblock.writes.retry` under its request id's `UUID` is taken like any due retry, and `apply`'s
> row-keyed handling throws a `ClassCastException` out of the cycle, which `run` only logs. That
> leftover and every envelope the cycle drained and had not yet written are neither applied,
> rescheduled nor dead-lettered. The map has to be drained, or cleared, before this consumer first
> starts against it.
>
> - Affected: `src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:259` - `cycle`, `:310` -
>   `apply`
> - Type: **RISK**
> - Status: **OPEN**

> #### A superseded write is lost when the write superseding it is dead-lettered
> A fresh envelope drops its row's waiting write before it is applied, and logs the two request
> ids. The fresh write states the row's whole final state, so the older one is not needed while the
> newer one lands. If the newer one fails every attempt instead - an upsert whose plain link names a
> row that never appears, say - it is dead-lettered, and the older write, which could have landed, is
> gone: the row keeps its state from before both, and the log line is the only record of the older
> write.
>
> - Affected: `src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:243` - `cycle`
> - Type: **RISK**
> - Status: **OPEN**

> #### A waiting retry still reverts a write that reaches the corpus outside the queue
> A waiting write is dropped only when a newer queued write of its row arrives. A write that reaches
> the corpus by another path while a retry waits out its backoff - a commit to `data/v1` by hand or
> through a pull request, or a session writing through `SkyBlockData.writing(...)` - is not seen,
> and the retry replaces that row whole when it lands, up to 31 minutes later at the configured five
> attempts. No deployment in the workspace writes the corpus outside this consumer.
>
> - Affected: `src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:259` - `cycle`, `:391` -
>   `reschedule`
> - Type: **RISK**
> - Status: **OPEN**

> #### A failed group written one envelope at a time splits rows that only pass together
> When a group of more than one envelope fails, `apply` writes each envelope on its own and
> reschedules only those that fail alone. Rows that pass the link check only together - upserts of
> one type naming each other through plain `@Linked` fields, or deletes of rows that name only each
> other - are each refused on their own, go back to wait, and land only once they fall due in one
> drain with no failing envelope grouped beside them. No skyblock model declares a plain link into
> its own type, so nothing reaches this today.
>
> - Affected: `src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:328` - `apply`
> - Type: **RISK**
> - Status: **OPEN**
