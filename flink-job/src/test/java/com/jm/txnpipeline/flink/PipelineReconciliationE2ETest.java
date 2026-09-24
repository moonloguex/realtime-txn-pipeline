package com.jm.txnpipeline.flink;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the real topology against Postgres -> Debezium -> Kafka -> Flink -> ClickHouse in
 * containers, then reconciles every ClickHouse result against Postgres, the source of truth.
 * Automates the manual checks in ARCHITECTURE_DECISIONS.md §10.
 */
@Tag("e2e")
@Testcontainers
class PipelineReconciliationE2ETest {

    private static final Path INFRA = Path.of("..", "infra");
    private static final String TOPIC = "cdc.public.transactions";
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    // Far enough past the data to push the watermark beyond every data window; its own window
    // never closes, so it is excluded from the window comparison.
    private static final Instant SENTINEL = T0.plusSeconds(600);

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16")
            .withNetwork(NET).withNetworkAliases("postgres")
            .withDatabaseName("bankdb").withUsername("bankuser").withPassword("bankpass")
            .withCommand("postgres", "-c", "wal_level=logical")
            .withCopyFileToContainer(MountableFile.forHostPath(INFRA.resolve("schema.sql")),
                    "/docker-entrypoint-initdb.d/schema.sql");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1")
            .withNetwork(NET).withNetworkAliases("kafka")
            .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> CONNECT = new GenericContainer<>("debezium/connect:2.7.3.Final")
            .withNetwork(NET)
            .dependsOn(KAFKA, POSTGRES)
            .withEnv(Map.of(
                    "BOOTSTRAP_SERVERS", "kafka:19092",
                    "GROUP_ID", "e2e-connect",
                    "CONFIG_STORAGE_TOPIC", "e2e_connect_configs",
                    "OFFSET_STORAGE_TOPIC", "e2e_connect_offsets",
                    "STATUS_STORAGE_TOPIC", "e2e_connect_statuses"))
            .withExposedPorts(8083)
            .waitingFor(Wait.forHttp("/connectors").forPort(8083));

    @Container
    static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>("clickhouse/clickhouse-server:24-alpine")
            .withExposedPorts(8123)
            .withCopyFileToContainer(MountableFile.forHostPath(INFRA.resolve("clickhouse-schema.sql")),
                    "/tmp/clickhouse-schema.sql")
            .waitingFor(Wait.forHttp("/ping").forPort(8123));

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    void clickhouseResultsReconcileWithPostgres() throws Exception {
        var schema = CLICKHOUSE.execInContainer("sh", "-c", "clickhouse-client --multiquery < /tmp/clickhouse-schema.sql");
        assertEquals(0, schema.getExitCode(), schema.getStderr());

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            // pre-created so the Flink source finds the partition at startup instead of waiting
            // for the next partition discovery
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
        registerConnectorAndWaitForStreaming();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2_000);
        String clickhouseUrl = "jdbc:clickhouse://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123) + "/default";
        TransactionProcessor.buildPipeline(env, KAFKA.getBootstrapServers(), clickhouseUrl);
        JobClient job = env.executeAsync("e2e");
        try {
            insertTransactions();

            Expected expected = expectedFromPostgres();
            Expected actual = null;
            for (int i = 0; i < 90 && !expected.equals(actual); i++) {
                Thread.sleep(2_000);
                actual = actualFromClickhouse();
            }

            assertEquals(expected.windows, actual.windows, "1-minute windows vs Postgres");
            assertEquals(expected.balances, actual.balances, "balances vs Postgres running sum");
            assertEquals(expected.largeAmount, actual.largeAmount, "large_amount flags vs Postgres");
            assertEquals(expected.highVelocity, actual.highVelocity, "high_velocity flags vs Postgres");
            // FINAL hides duplicates; the raw tables must have none either
            assertEquals("0\t0", clickhouse(
                    "SELECT (SELECT count() - uniqExact(account_id, window_start) FROM windowed_txn_stats),"
                            + " (SELECT count() - uniqExact(account_id, reason, detected_at) FROM anomaly_flags)"));
            System.out.printf("reconciled: %d windows, %d balances, %d large_amount, %d high_velocity%n",
                    actual.windows.size(), actual.balances.size(), actual.largeAmount.size(), actual.highVelocity.size());
        } finally {
            job.cancel().get();
        }
    }

    private void registerConnectorAndWaitForStreaming() throws Exception {
        String connectUrl = "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + "/connectors";
        HttpResponse<String> res = HTTP.send(HttpRequest.newBuilder(URI.create(connectUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Files.readString(INFRA.resolve("connector-config.json"))))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(201, res.statusCode(), res.body());

        // Rows inserted before the snapshot finishes arrive as op=r, which the parser skips by
        // design; wait until the replication slot is streaming.
        try (Connection pg = postgres()) {
            for (int i = 0; i < 60; i++) {
                try (ResultSet rs = pg.createStatement().executeQuery(
                        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'txn_slot' AND active")) {
                    rs.next();
                    if (rs.getInt(1) == 1) {
                        return;
                    }
                }
                Thread.sleep(1_000);
            }
        }
        throw new IllegalStateException("Debezium never started streaming");
    }

    private static void insertTransactions() throws Exception {
        Random rnd = new Random(42);
        try (Connection pg = postgres();
             PreparedStatement ps = pg.prepareStatement(
                     "INSERT INTO transactions (account_id, amount, created_at) VALUES (?, ?, ?)")) {
            long offsetMs = 0;
            for (int i = 0; i < 300; i++) {
                offsetMs += 500 + rnd.nextInt(1_500); // generator's 0.5-2s cadence
                add(ps, 1 + rnd.nextInt(5), BigDecimal.valueOf(rnd.nextInt(10_000_000) - 5_000_000, 2), T0.plusMillis(offsetMs));
            }
            for (int i = 1; i <= 4; i++) { // guaranteed high_velocity burst
                add(ps, 3, new BigDecimal("10.00"), T0.plusMillis(offsetMs + i * 100L));
            }
            add(ps, 1, new BigDecimal("1.00"), SENTINEL);
            ps.executeBatch();
        }
    }

    private static void add(PreparedStatement ps, int account, BigDecimal amount, Instant at) throws Exception {
        ps.setInt(1, account);
        ps.setBigDecimal(2, amount);
        ps.setTimestamp(3, Timestamp.from(at));
        ps.addBatch();
    }

    private record Expected(Set<String> windows, Set<String> balances, Set<String> largeAmount, Set<String> highVelocity) {
    }

    private static Expected expectedFromPostgres() throws Exception {
        long sentinelMinute = SENTINEL.getEpochSecond() / 60 * 60;
        try (Connection pg = postgres()) {
            return new Expected(
                    pgRows(pg, "SELECT account_id, extract(epoch FROM date_trunc('minute', created_at))::bigint, count(*), sum(amount)"
                            + " FROM transactions WHERE created_at < to_timestamp(" + sentinelMinute + ") GROUP BY 1, 2"),
                    pgRows(pg, "SELECT account_id, sum(amount) FROM transactions GROUP BY 1"),
                    pgRows(pg, "SELECT account_id, (extract(epoch FROM created_at) * 1000)::bigint"
                            + " FROM transactions WHERE abs(amount) > 47500"),
                    // same rule, independently restated in SQL: >= 4 txns on the account within
                    // the inclusive 5s window ending at this txn
                    pgRows(pg, "SELECT account_id, (extract(epoch FROM created_at) * 1000)::bigint FROM ("
                            + " SELECT account_id, created_at, count(*) OVER (PARTITION BY account_id ORDER BY created_at"
                            + "   RANGE BETWEEN interval '5 seconds' PRECEDING AND CURRENT ROW) AS n FROM transactions) t"
                            + " WHERE n >= 4"));
        }
    }

    private static Expected actualFromClickhouse() throws Exception {
        long sentinelMinute = SENTINEL.getEpochSecond() / 60 * 60;
        return new Expected(
                chRows("SELECT account_id, toUnixTimestamp(window_start), txn_count, txn_amount FROM windowed_txn_stats FINAL"
                        + " WHERE window_start < toDateTime(" + sentinelMinute + ")"),
                chRows("SELECT account_id, balance FROM balance_snapshot FINAL"),
                chRows("SELECT account_id, toUnixTimestamp64Milli(detected_at) FROM anomaly_flags FINAL WHERE reason = 'large_amount'"),
                chRows("SELECT account_id, toUnixTimestamp64Milli(detected_at) FROM anomaly_flags FINAL WHERE reason = 'high_velocity'"));
    }

    private static Set<String> pgRows(Connection pg, String sql) throws Exception {
        Set<String> rows = new TreeSet<>();
        try (Statement st = pg.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= cols; c++) {
                    row.append(c > 1 ? "|" : "").append(normalize(rs.getString(c)));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    private static Set<String> chRows(String sql) throws Exception {
        Set<String> rows = new TreeSet<>();
        for (String line : clickhouse(sql).split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            StringBuilder row = new StringBuilder();
            String[] cols = line.split("\t");
            for (int c = 0; c < cols.length; c++) {
                row.append(c > 0 ? "|" : "").append(normalize(cols[c]));
            }
            rows.add(row.toString());
        }
        return rows;
    }

    // Postgres prints NUMERIC as "-120.50", ClickHouse prints Decimal as "-120.5"
    private static String normalize(String v) {
        return new BigDecimal(v).stripTrailingZeros().toPlainString();
    }

    private static String clickhouse(String sql) throws Exception {
        String url = "http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123)
                + "/?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
        HttpResponse<String> res = HTTP.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode(), res.body());
        return res.body().trim();
    }

    private static Connection postgres() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
