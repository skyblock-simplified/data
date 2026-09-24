# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See the root [`CLAUDE.md`](../CLAUDE.md) for cross-cutting patterns, dependency details, and the
locked [data + Hazelcast initiative](../../../.claude/projects/W--Workspace-Java-SkyBlock-Simplified/memory/architecture_simplified_data_initiative.md)
in the auto-memory.

## Build & Test

```bash
# From repo root
./gradlew :data:build          # Build (includes shadowJar)
./gradlew :data:test           # Run all tests

# The Spring Boot context test needs a live Hazelcast cluster on skyblock-hazelcast-net and
# SKYBLOCK_DATA_GITHUB_TOKEN set, so it is skipped unless SKYBLOCK_HAZELCAST=true.
SKYBLOCK_HAZELCAST=true ./gradlew :data:test

# Fat JAR
./gradlew :data:shadowJar      # Output: build/libs/data-0.1.0.jar
```

## Module Overview

`data` is the autonomous data writer service for the SkyBlock-Simplified initiative: a Spring
Boot context that drains the `skyblock.writes` IQueue on the docker cluster defined in
`infra/hazelcast/` and applies each write to the corpus - `data/v1` in the `simplified-api/skyblock`
repo - through the corpus's writable source. It opens no database and holds no second-level cache; the Hazelcast client carries the
write queue, its retry map and its dead-letter map.

### Phase scope tracker

| Phase | Status | Scope |
|---|---|---|
| 2c | done | Spring Boot scaffold + Hazelcast client wiring + L2 cache validation |
| 2d | done | Lazy-streaming JpaRepository rewrite, query cache disabled under Hazelcast |
| 3 | done | skyblock-data repo populated (42 JSONs, 41 entities, manifest generator, CI) |
| 4a | done | Library-side Source interface foundation (Simplified-Dev/persistence) |
| 4b | done | GitHub integration: SkyBlockDataContract + GitHubIndexProvider + GitHubFileFetcher + GitHubConfig |
| 4c | done | Scheduled AssetPoller (watchdog-only) + AssetDiffEngine + dedicated asset-state JpaSession |
| 5 | done | Switch SkyBlock repositories to RemoteSkyBlockFactory (DiskOverlaySource + RemoteJsonSource) |
| 5.5 | done | AssetPoller targeted refresh trigger via `RefreshTrigger` SAM + `JpaSession.refreshModels` library API. Changes propagate on the next 60s poll instead of waiting for restart. |
| 5.5.1 | done | Deleted obsolete Phase 4b ETagContext workaround after `Simplified-Dev/client` auto-attaches `If-None-Match` and transparently serves cached bodies on 304. Net -160 lines across GitHubConfig, AssetPoller, SkyBlockDataException, AssetPollerTest. |
| 5.5.2 | done | Hotfix: switch `SkyBlockDataContract.getLatestMasterCommit` from `/commits?sha=master&per_page=1` (stale edge cache, observed 10+ min lag) to `/commits/master` (always fresh via git ref path). Return type collapsed to single `GitHubCommit`. Phase 5.5 gate-6 end-to-end verified live against docker. |
| 6a | done | `WriteRequest` envelope class + 9 unit tests in `Simplified-Dev/persistence`. Plain Java `Serializable`, pre-serialized `entityJson`, `UPSERT`/`DELETE` nested enum. Library foundation for the IQueue write path. |
| 6b.4 | done | **Prometheus + Grafana observability stack**. New `infra/prometheus/` directory with a standalone docker-compose stack that attaches to the existing `skyblock-hazelcast-net` bridge network as `external: true`. `prom/prometheus:v2.54.1` scrapes `http://data:8080/actuator/prometheus` every 15s via docker-internal DNS - port 9090 is exposed on the private network only, never published to the host. `grafana/grafana:11.4.0` published to loopback `127.0.0.1:3000` only, admin password driven by an optional `${GRAFANA_ADMIN_PASSWORD}` env var. `alerts.yml` loads 6 rules covering the Phase 6b.3 meter catalog: 2 pages (`SkyBlockWritesDeadLetterPage` fires on any `increase(skyblock_writes_deadletter_added_total[15m]) > 0`, `SkyBlockWritesRetryImapCritical` fires when `skyblock_writes_retry_imap_size > 1000` for 1m) and 4 warns (`SkyBlockWritesRetryImapBacklog > 100 for 5m`, `SkyBlockWritesCommitFailureRate > 5% for 10m`, `SkyBlockWritesCommitLatencyHigh` p95 > 10s for 10m, `SkyBlockWritesConsumerDrainStalled` absent-over-time liveness check). Grafana datasource provisioning pins `uid: prometheus` so the write-path dashboard references it stably. Write-path dashboard JSON has 8 panels: throughput, commit latency quantiles by mode, retry attempts stacked by attempt counter, buffer depth top 10 by entity type, dead-letter rate by type, Hazelcast IMap/IQueue sizes, Git Data API per-step p95 duration, and commit failures by mode/reason. `allowUiUpdates: true` on the dashboard provider so operators can iterate layouts in the UI. **Scope NOT in 6b.4**: Alertmanager wiring (deferred until a paging target is chosen - alerts are visible at `http://localhost:9090/alerts` only and do not deliver anywhere), node-exporter / cAdvisor, Hazelcast native Prometheus exporter, bot / server scrape jobs. Infra-only phase, zero changes to Java, Gradle, or the existing hazelcast compose stack. Commits live on the parent repo (`72e81c6` + `422da31` on SkyBlock-Simplified `master`), phase tracker flip on this nested repo. |
| 6b | done | **Write path wiring**: `WritableRemoteJsonSource<T>` wraps the `DiskOverlaySource(RemoteJsonSource)` chain and layers GitHub Contents API PUT on top. `SkyBlockDataWriteContract` + new DTOs + separate `skyBlockDataWriteClient` bean with `Accept: application/vnd.github+json`. `WriteQueueConsumer` daemon thread drains `skyblock.writes` IQueue + a local `DelayQueue<DelayedWriteRequest>` for exponential-backoff retries; dead-letter to `skyblock.writes.deadletter` IMap after 5 attempts. `WriteBatchScheduler` fires every 10s and escalates failures to the consumer's retry queue. `skyBlockWriteHazelcastInstance` bean in `PersistenceConfig` (second client alongside the JCache-managed instance). `RemoteSkyBlockFactory` wraps every per-model source in a `WritableRemoteJsonSource` and exposes a typed `getWritableSources()` registry for the write-path beans. `SmokeWriteSentinel` bean activated under `@Profile("smoke")` for gate-7 end-to-end docker testing. Dormant `SkyBlockGitDataContract` + 8 Git Data API DTOs shipped as an extractable API surface with compile-only DTO round-trip tests - no production caller in Phase 6b, reserved for Phase 6b.1. |
| 6b.3 | done | **Micrometer meters for the write path**. New `WriteMetrics` `@Component` in `write/` that owns every meter exposed by `data`: 8 counters, 4 timers, 4 gauges. Bounded-enum tag types (`CommitMode`, `SkipReason`, `FailureReason`, `GitDataStep`) enforce cardinality discipline. Per-type buffer depth gauges bounded at 41 (one per SkyBlock entity), dead-letter counter tagged by FQCN (same bound), no request ids in any tag. `WriteMetricsTest` covers every helper method via Micrometer's in-memory `SimpleMeterRegistry`. **Dependency shape change**: dropped `spring-boot-starter` + `spring-boot-starter-actuator` in favour of `server-api` which transitively exports `spring-boot-starter-web` and `spring-boot-starter-actuator` via `api()` coordinates, matching `server`. Added `micrometer-registry-prometheus` as an explicit catalog dep pinned to 1.14.5. Flipped `spring.main.web-application-type` from `none` to `servlet` so the `/actuator/prometheus` scrape endpoint is reachable on port 8080 (private to `skyblock-hazelcast-net`, never published to host). Disabled `api.key.authentication.enabled` because data has no REST endpoints to protect. Spring Boot version bumped 3.4.4 -> 3.4.5 to match `server-api`'s catalog. `SimplifiedData` main class now scans `dev.sbs.serverapi` in addition to `dev.sbs.data` so the framework's auto-config beans register. Wired `WriteMetrics` into `WriteQueueConsumer` (fresh/retry dispatch counters, dispatch latency timer, skip/dead-letter counters, 3 Hazelcast-backed gauges registered in `start()`), `WriteBatchScheduler` (commit duration timer + end-to-end latency per mutation + escalation counter, both commit modes), `GitDataCommitService` (per-step Timer.Sample around each of the 6 Git Data API steps + retries-exhausted counter on budget exhaustion), `WritableRemoteJsonSource` (per-type buffer depth gauge registered in the constructor), `RemoteSkyBlockFactory` (threads `WriteMetrics` through the per-model `buildSourceFor` helper), and `PersistenceConfig` (injects `WriteMetrics` into the `remoteSkyBlockFactory` bean method). |
| 6b.2 | done | **WriteRequest.withRequestId hygiene cleanup**: drop the reflection-based `rebuildWithRequestId` static helper in `WriteBatchScheduler` (unwrapped the private `WriteRequest` constructor to preserve the original producer's request id across retry cycles) in favour of a new library-side `WriteRequest.withRequestId(UUID)` instance method on `Simplified-Dev/persistence`. Net -55 lines on the service side, +37 lines (library method + unit test) on the library side. Runtime behavior byte-identical - same UUID substitution, same field preservation, just via a proper library API instead of reflection on `getDeclaredConstructor`. |
| 6b.1 | done | **Git Data API production path + dual-file routing + gap fixes**: `WritableRemoteJsonSource.stageBatch()` new two-phase commit entry point that reads both primary and `_extra` files via `FileFetcher`, routes mutations to the owning file by id (extras wins on UPSERT, DELETE removes from BOTH files on conflict, new ids default to primary), and returns a `StagedBatch` with only the files that actually changed (suppresses byte-identical no-ops). `GitDataCommitService` (@Component) orchestrates the 7-step Git Data API flow (getRef → getCommit → createBlob*N → createTree(base_tree) → createCommit → updateRef) with 3 immediate retries on 409/422 non-fast-forward before escalating. `WriteBatchScheduler.tick()` dispatches on the `skyblock.data.github.write-mode` property: `GIT_DATA` (default) uses a two-phase staging + single-commit path that produces exactly one commit per tick across every dirty file on every source; `CONTENTS` retains the Phase 6b per-file commit path as an operational fallback. Commit message in GIT_DATA mode matches the original Q4 format: `"Batch update: <N> entities across <M> files"` header with per-entity-class breakdown in the body. Two known-gap fixes landed after the initial 6b.1 commits: (1) **DELETE bug** for cross-file id conflicts now removes from both primary and extras files (previously only removed from extras, leaving the stale primary visible via the read-path append merge). (2) **Durable retry state** migrated from in-process `DelayQueue<DelayedWriteRequest>` to Hazelcast `IMap<UUID, RetryEnvelope> skyblock.writes.retry` so in-flight exponential-backoff retries survive consumer crash/restart. The migration also fixes a latent attempt-counter bug where `escalateStagedBatch` hardcoded `attempt=1` on every escalation - the new path reads `mutation.getAttempt()` (new field on `BufferedMutation`) and correctly computes `nextAttempt = current + 1`, producing a bounded retry chain that terminates at `maxRetryAttempts` instead of looping forever. Fixes the items_extra.json routing bug that Phase 6b had flagged as a known limitation. |
| 6c | done | **bot producer SDK**. New `dev.sbs.bot.write.WriteDispatcher` class + classpath `hazelcast-client.xml` + unit tests. Thin pass-through: builds `WriteRequest` envelopes via the library factories, serializes entities through a caller-supplied Gson, enqueues on the `skyblock.writes` IQueue via blocking `put()`, re-asserts the interrupt flag and wraps `InterruptedException` in `IllegalStateException` on interrupt. Two construction modes: caller-owned `HazelcastInstance` (public constructor, `close()` is a no-op for Hazelcast) and self-owned (`createClient()` static factory that mints a fresh `HazelcastClient.newHazelcastClient()` from the classpath XML and pulls Gson from `MinecraftApi.getGson()`, `close()` shuts down the owned instance). No eager wiring in `SimplifiedBot.main()` or `bootstrap()` - SDK only per the "speculatively ship without concrete write triggers" scope. `hazelcast-client.xml` mirrors `infra/hazelcast/hazelcast-client.xml` with a sync warning and a note that operators running bot on the host must override via `hazelcast.client.config` system property. 9 unit tests via Proxy-stubbed `HazelcastInstance` + `IQueue`: upsert/delete envelope field correctness, Gson round-trip of entity json, pre-built envelope passthrough, getter accessors, constant values, request id uniqueness per call, close() ownership (owned vs external), and interrupt handling. Verification was run via temporary sourceSets exclusions because bot is mid-refactor at HEAD with unrelated compile failures in `profile_stats`, `optimizer`, and related packages driven by an in-progress ChatFormat/asset-renderer migration; the committed `build.gradle.kts` is surgical (only adds the hazelcast dep). |
| 6d | done | Operator runbook: `SKYBLOCK_DATA_GITHUB_TOKEN` was upgraded to `contents:write` scope during the Phase 6b.1 session (inline operator action on the operator-owned `infra/hazelcast/.env`, no code artifact). Docker-compose env var plumbing landed earlier in Phase 5.5.2 (commit `5d7674f` on parent). This row was a staleness cleanup in the same commit as the Phase 6c flip. |
| 7 | done | **Local SkyBlock JSONs removed**. Deleted the 42 resource JSONs under `minecraft-api/src/main/resources/skyblock/` (41 primary + 1 `items_extra.json` companion, 209928 lines). `SkyBlockFactory.defaultSource = Source.json("skyblock")` continues to work without code changes - `JsonSource.load()` gracefully returns empty `ConcurrentList` on missing classpath resources. data production runtime is unaffected (uses `RemoteSkyBlockFactory` -> `DiskOverlaySource(RemoteJsonSource)` per model from the `skyblock-data` GitHub repo). Consumers of the local path (minecraft-api `JpaModelTest`, data `JpaModelHazelcastTest`, `SimplifiedBot.main()`'s `connectSkyBlockSession()` call) continue to compile but see empty repositories at runtime; those callers are all already part of an in-progress refactor WIP (ChatFormat/asset-renderer migration + NBT API change) that will need to adapt them separately. Post-deletion docker boot verified: 41 models wired from remote, Phase 5.6 cold-boot skip firing correctly, AssetPoller scheduled cadence unchanged. Commit: `8576838` on minecraft-api master. |

### Entry Point

- **`SimplifiedData`** - the Spring Boot application, and the shadow jar's `Main-Class`. It runs a
  servlet container on 8080 inside the private `skyblock-hazelcast-net` docker network, never
  published to the host, which serves Actuator's `health`, `info`, `metrics` and `prometheus`
  endpoints, Spring Boot's `/error` and the `/login` and `/logout` of Spring Security's default
  chain - no controller of its own. It holds no `JpaSession` and opens no database:
  `PersistenceConfig` wires the corpus and a Hazelcast client to the dockerized cluster, and
  `WriteQueueConsumer` writes straight through the corpus's writable source.
- `scanBasePackages` names `dev.sbs.data` and `dev.sbs.serverapi`. The spring-framework library
  this module depends on declares its configuration under `dev.simplified.serverapi`, which that
  scan does not name, so none of the library's `@Configuration` classes is picked up by it -
  including `PermitAllSecurityConfig`, which `api.key.authentication.enabled=false` in
  `application.properties` is meant to select. With no `SecurityFilterChain` of the application's
  own, the default chain of Spring Boot's `ManagementWebSecurityAutoConfiguration` applies: it
  permits the health endpoint, requires authentication for every other request,
  `/actuator/prometheus` included, and turns on HTTP basic and form login, whose generated login
  and logout pages answer at `/login` and `/logout`.

### Package Structure

Everything is under `dev.sbs.data`:

- **`SimplifiedData`** - the entry point above.
- **`DataApi`** - service locator for this deployment's `GsonSettings.defaults()` and the `Gson`
  it creates. `GsonSettings.defaults()` picks up every `ServiceLoader` contributor on the
  classpath, so nothing registers an adapter by hand; `WriteQueueConsumer` decodes each queued
  row with this `Gson`.

- **`config/`**:
  - `PersistenceConfig` - two beans. `skyBlockCorpus` is `SkyBlockData.corpus()` named with the
    token `GitHubToken.of` reads from `SKYBLOCK_DATA_GITHUB_TOKEN` (`TOKEN_VARIABLE`) through
    `SystemUtil.getEnv`, described under Environment variables below. It throws as the bean is
    built when the variable is missing or empty; a value of whitespace alone is taken and sent.
    `skyBlockWriteHazelcastInstance` is `HazelcastClient.newHazelcastClient()` over the classpath
    `hazelcast-client.xml` (cluster `skyblock`, member `hazelcast:5701`), shut down by a
    `@PreDestroy` method when the context closes.

- **`write/`**:
  - `WriteQueueConsumer` - `@Component` applying queued writes to the corpus through the
    `Source.Writable` that `SkyBlockData.writing(corpus)` returns. On `ApplicationReadyEvent`
    it registers the depth gauges and starts the daemon thread `skyblock-write-drain`, unless
    `skyblock.data.github.write-consumer-enabled` is `false`; its `@PreDestroy` method
    interrupts the thread and waits up to two seconds for it. Each `cycle()` polls the
    `skyblock.writes` `IQueue` for up to 500 ms for one fresh `WriteEnvelope`, then takes every
    entry of the `skyblock.writes.retry` `IMap` whose ready instant has passed, removing each
    before dispatching it so a second consumer cannot take it too. The drained envelopes are
    grouped by type name and operation, and each group is one `WriteRequest` - an upsert or a
    delete over the group's rows, decoded with `DataApi`'s `Gson` - written through the source.
    A failed group's envelopes go back on the retry map with the attempt raised by one and a
    ready instant from `RetryEnvelope.computeReadyAt`; an envelope whose attempt would pass
    `skyblock.data.github.write-retry-max-attempts` goes to the `skyblock.writes.deadletter`
    `IMap` instead, for an operator. A package-private constructor takes the `Source.Writable`
    directly, which is how `WriteQueueConsumerTest` puts a recording source under it.
  - `RetryEnvelope` - the `Serializable` value the retry map holds: the `WriteEnvelope`, the
    attempt it represents (the original dispatch is attempt zero) and the epoch-millis instant
    it becomes ready. `computeReadyAt` doubles the delay each attempt, starting from
    `skyblock.data.github.write-retry-initial-delay-minutes`. The map lives in the cluster, so a
    restart of this service picks pending retries back up.
  - `WriteMetrics` - `@Component` holding every meter the write path publishes for the
    Prometheus scrape: the counters `skyblock.writes.requests.received`,
    `skyblock.writes.requests.retried` (tagged `attempt`) and `skyblock.writes.deadletter.added`
    (tagged `type`); the timers `skyblock.writes.duration` (tagged `status`, `success` or
    `failure`) and `skyblock.writes.end_to_end.latency`; and the gauges
    `skyblock.writes.primary_queue.size`, `skyblock.writes.retry_imap.size` and
    `skyblock.writes.deadletter_imap.size`.

`WriteEnvelope`, the queued wire format, is not in this module. It lives in the shared
`SkyBlock-Simplified/api` library as `dev.sbs.api.write.WriteEnvelope`, so the producer and this
consumer read one definition.

Resources: `application.properties` carries the servlet and Actuator settings,
`api.key.authentication.enabled=false` and the three `skyblock.data.github.write-*` properties;
`hazelcast-client.xml` names the client's cluster and member address; `logback.xml` configures
logging.

Tests: `WriteQueueConsumerTest` drains against a real in-process Hazelcast member, configured in
code with a random cluster name and discovery off, over a recording source rather than GitHub.
`SimplifiedDataApplicationTests` is the context-loads test gated on `SKYBLOCK_HAZELCAST`.

`src/test/resources/hazelcast.xml` is a member configuration - cluster `skyblock-test`, port 5801
with auto-increment over 20 ports, every join mechanism off - that nothing loads.
`WriteQueueConsumerTest` passes its `Config` to `Hazelcast.newHazelcastInstance(Config)`, which
reads no file, and nothing calls the no-argument form that would look for a classpath
`hazelcast.xml`. The context the gated test starts holds one Hazelcast instance, the client
`PersistenceConfig` builds from `hazelcast-client.xml`, and none of the Spring Boot 4.0.5 modules
on the test classpath carries a Hazelcast auto-configuration that would build a member from the
file.

### Dependencies

Every Simplified coordinate is pinned `strictly` to a sha except `SkyBlock-Simplified/api`, taken
as `master-SNAPSHOT`; building from the workspace root substitutes the local checkouts for all of
them.

- **`skyblock`** (`com.github.simplified-api:skyblock`) - the corpus models and `SkyBlockData`, whose
  `corpus()` names the published corpus and whose `writing(corpus)` answers the writable source
  `WriteQueueConsumer` applies every write through.
- **`github`** (`com.github.simplified-api:github`) - `GitHubCorpus` and `GitHubToken`, reached
  directly because this deployment is the one that holds a write token.
- **`api`** (`com.github.skyblock-simplified:api`) - `WriteEnvelope`, the envelope the write queue
  carries, shared with the producer.
- **`persistence`** (`com.github.simplified-dev:persistence`) - `Source.Writable`, `WriteRequest`
  and `JpaModel`, the terms a corpus write is made in.
- **`spring-framework`** (`com.github.simplified-dev:spring-framework`) - exports Spring Boot 4's
  `spring-boot-starter-web`, `spring-boot-starter-actuator` and `spring-boot-starter-security`
  through `api()`, so this module declares no Spring Boot starter of its own.
- **`micrometer-registry-prometheus` 1.16.4** - Spring Boot serves `/actuator/prometheus`, the
  scrape output every meter renders to, only with the registry on the classpath, and the actuator
  starter does not carry it. This module imports no Spring Boot BOM, so the catalog pins it, at
  the Micrometer line Spring Boot 4.0.5 manages: it resolves with the `micrometer-core` 1.16.4 the
  actuator starter brings, and brings the `prometheus-metrics` 1.4.3 artifacts, the Prometheus
  client version Spring Boot 4.0.5 manages.
- **`com.hazelcast:hazelcast` 5.6.0** - the client that carries the write queue, its retry map
  and its dead-letter map, and the in-process member `WriteQueueConsumerTest` drains against.
- **`client`**, **`gson-extras`** and **`collections`** (`com.github.simplified-dev`) - the HTTP
  client the corpus calls through, the `GsonSettings` behind `DataApi`, and the `Concurrent`
  collections.
- Tests use JUnit 5, Hamcrest and `spring-boot-starter-webmvc-test`.

### Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SKYBLOCK_DATA_GITHUB_TOKEN` | required | unset | Fine-grained PAT with `contents:write` on the `simplified-api/skyblock` repo, which carries the corpus. Nothing reads the corpus at startup: the token authenticates the requests each queued write makes - the catalogue refresh and the layer reads before it rewrites a document, then the PUT that rewrites it - and lifts them off the 60 req/hr unauthenticated budget. A missing or empty variable fails context refresh when the `skyBlockCorpus` bean is built, before any request, while one of whitespace alone is taken and sent; a token that is expired or lacks write scope shows up as a failed write, which the queue retries and then dead-letters, not as a failed boot. |
| `SKYBLOCK_HAZELCAST` | optional | unset | When set to `true`, enables the Spring context-loads test in `SimplifiedDataApplicationTests`, which needs a live Hazelcast cluster and `SKYBLOCK_DATA_GITHUB_TOKEN`; otherwise the test reports as skipped. Does not affect production behavior. |

`PersistenceConfig.skyBlockCorpus()` reads the token variable, named by
`PersistenceConfig.TOKEN_VARIABLE`, through `GitHubToken.of`, which looks it up with
`dev.simplified.util.SystemUtil.getEnv`: the name is matched case-insensitively against the OS
environment laid over two `.env` sources, the class-loader resource `../.env` and a `.env` in the
directory holding the jar or class directory `SystemUtil` was loaded from. In the shadow jar that
directory is the jar's own, `/app` in the image, which the `Dockerfile` puts no `.env` in. No
Spring property carries the token. The corpus client holds it and sends it as an
`Authorization: Bearer` header on every request it makes.
