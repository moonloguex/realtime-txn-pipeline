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
    implementation("com.clickhouse:clickhouse-jdbc:0.8.6:http") {
        exclude(group = "org.lz4", module = "lz4-java")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.7")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
