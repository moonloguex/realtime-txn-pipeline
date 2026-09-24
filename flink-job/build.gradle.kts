plugins {
    id("application")
}

group = "com.jm.txnpipeline"
version = "0.1.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        // Flink 1.20 is most stable on Java 17 (FDS uses Java 21, kept separate here on purpose)
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

val flinkVersion = "1.20.4"

application {
    mainClass.set("com.jm.txnpipeline.flink.TransactionProcessor")
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20") {
        exclude(group = "org.lz4", module = "lz4-java")
    }
    implementation("org.apache.flink:flink-connector-jdbc:3.3.0-1.20")
    implementation("com.clickhouse:clickhouse-jdbc:0.9.8") {
        exclude(group = "org.lz4", module = "lz4-java")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.7")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    // operator test harnesses (KeyedOneInputStreamOperatorTestHarness etc.)
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.postgresql:postgresql:42.7.13")
}

// Unit tests only; the Docker-backed pipeline test runs via `gradle e2eTest`.
tasks.test {
    useJUnitPlatform { excludeTags("e2e") }
}

tasks.register<Test>("e2eTest") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("e2e") }
    testLogging { showStandardStreams = true }
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
