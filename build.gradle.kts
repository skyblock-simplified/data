plugins {
    id("java")
    id("com.gradleup.shadow") version "8.3.6"
}

group = "dev.sbs"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    maven(url = "https://jitpack.io")
}

dependencies {
    // Simplified Annotations
    compileOnly(libs.simplified.annotations)
    annotationProcessor(libs.simplified.annotations)
    testCompileOnly(libs.simplified.annotations)
    testAnnotationProcessor(libs.simplified.annotations)

    // Lombok Annotations

    // Tests
    testImplementation(libs.hamcrest)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.junit.platform.launcher)
    testImplementation(libs.spring.boot.starter.test)

    // Server framework - its api() exports supply the Spring Boot starters this service runs
    // on: spring-boot-starter-web, whose servlet container serves the Actuator endpoints on
    // 8080, spring-boot-starter-actuator, which exposes /actuator/prometheus for the scrape,
    // and spring-boot-starter-security, so the only starter declared here is the test one.
    // The library exports client and gson-extras as well. application.properties sets
    // api.key.authentication.enabled=false: the only endpoints served are Actuator's, and
    // this module has no controller of its own for the library's API key authentication to
    // protect.
    implementation("com.github.simplified-dev:spring-framework") { version { strictly("6c1497b") } }

    // Micrometer Prometheus registry - renders every meter, WriteMetrics' included, as the
    // /actuator/prometheus output. The catalog carries its version, since data imports
    // no spring-boot-dependencies BOM that would supply one.
    implementation(libs.micrometer.registry.prometheus)

    // Hazelcast - the client carries the write queue, its retry map and its dead-letter
    // map. PersistenceConfig builds it from the classpath hazelcast-client.xml, and
    // WriteQueueConsumer drains the skyblock.writes IQueue, parks a failed write in the
    // skyblock.writes.retry IMap and moves a spent one to skyblock.writes.deadletter;
    // WriteMetrics gauges all three. The same jar runs the in-process member
    // WriteQueueConsumerTest drains against.
    implementation(libs.hazelcast)

    // gson-extras holds DataApi's GsonSettings; client is the HTTP client the corpus calls through
    implementation("com.github.simplified-dev:client") { version { strictly("2ced9a4") } }
    implementation("com.github.simplified-dev:gson-extras") { version { strictly("ed1d77e") } }

    // The SkyBlock corpus - SkyBlockData, whose corpus() names the published corpus and whose
    // writing(corpus) answers the writable source every queued write goes through, and the
    // corpus models. minecraft-text reaches the classpath through its api() exports.
    implementation("com.github.simplified-api:skyblock") { version { strictly("d566734") } }

    // The shared SkyBlock-Simplified library, which owns the envelope the write queue carries -
    // one definition of the wire format, spoken by the producer and by this consumer.
    implementation("com.github.skyblock-simplified:api:master-SNAPSHOT")

    // The corpus client - GitHubCorpus and its write instruction. Reached directly because this
    // deployment is the one that holds a token, and holding one is the whole of what makes it a
    // writer.
    implementation("com.github.simplified-api:github") { version { strictly("b64127c") } }

    implementation("com.github.simplified-dev:persistence") { version { strictly("2d6b0e7") } }
    implementation("com.github.simplified-dev:collections") { version { strictly("9696ca5") } }
}

tasks {
    withType<JavaCompile> {
        options.compilerArgs.add("-parameters")
    }

    test {
        useJUnitPlatform()
    }

    shadowJar {
        archiveClassifier.set("")
        mergeServiceFiles()
        append("META-INF/spring.factories")
        append("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")

        manifest {
            attributes["Main-Class"] = "dev.sbs.data.SimplifiedData"
            attributes["Multi-Release"] = "true"
        }

        exclude("META-INF/INDEX.LIST", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }

    build {
        dependsOn(shadowJar)
    }

    register<Exec>("deploy") {
        description = "Build and deploy to remote Docker host via SSH over VPN"
        group = "deployment"
        dependsOn(shadowJar)

        doFirst {
            project.file(".env").readLines()
                .filter { it.contains('=') && !it.startsWith('#') }
                .forEach { environment(it.substringBefore('='), it.substringAfter('=')) }
        }

        commandLine("docker", "compose", "up", "-d", "--build", "--remove-orphans")
    }
}
