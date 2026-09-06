package com.jm.txnpipeline.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * Reads Debezium CDC events for the `transactions` table from Kafka, keeps a running
 * balance per account, computes 1-minute windowed stats, flags simple anomalies, and
 * writes all three results to ClickHouse.
 *
 * Runs as a plain `main()` — Flink spins up an embedded local cluster, no separate
 * Flink deployment needed for this learning project.
 */
public class TransactionProcessor {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String KAFKA_TOPIC = "cdc.public.transactions";
    // http_keep_alive=false: ClickHouse's HTTP server closes idle connections after
    // keep_alive_timeout=10s (infra default), but the clickhouse-jdbc:0.8.6 Apache
    // HttpClient5 pool's stale-connection check (ahc_validate_after_inactivity, default
    // 5000ms) doesn't reliably catch every connection the server has already closed
    // before this sink reuses it - observed as sockets stuck in CLOSE_WAIT to :8123
    // (`lsof -p <pid> | grep 8123`) while the job ran continuously with no restarts.
    // A write on one of these racing/stale connections throws, JdbcOutputFormat retries
    // the whole batch (withMaxRetries below), and because the original request had often
    // already reached ClickHouse, the retry produces a genuine duplicate INSERT - this is
    // what caused the growing (account_id, reason, detected_at) duplicates in
    // anomaly_flags and the identical-value duplicate rows in windowed_txn_stats (see
    // _workspace/02_flink_rules.md §7). Disabling keep-alive means every insert opens a
    // fresh connection - negligible cost at this pipeline's throughput (batches every
    // 1-10s) - and removes the staleness race entirely rather than just narrowing it.
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default?http_keep_alive=false";
    private static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId("txn-flink-processor")
                // committedOffsets(EARLIEST), not earliest(): earliest() always replays the whole
                // topic on every cold start of this main()-run job (no savepoint restore wired up),
                // which is what caused millions of duplicate anomaly_flags rows (same account_id +
                // reason + detected_at repeated ~1200x — see _workspace/02_flink_rules.md). Once this
                // consumer group has committed progress (Flink commits offsets to Kafka on checkpoint
                // completion since checkpointing is enabled below), a restart resumes from there;
                // only a genuinely new group falls back to earliest.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TransactionEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "cdc-source")
                .flatMap(new DebeziumEventParser())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.createdAt.toEpochMilli())
                                // cdc.public.transactions is a single-partition topic (verified via
                                // `kafka-topics.sh --describe`; KAFKA_CFG_NUM_PARTITIONS is a Bitnami
                                // env var and has no effect on the apache/kafka image this stack uses,
                                // so the topic falls back to Kafka's 1-partition default). At ~0.8
                                // events/sec system-wide, and with generator gaps, that one partition
                                // still goes quiet often enough that without idleness the watermark
                                // never advances and no 1-minute tumbling window ever closes - this is
                                // why windowed_txn_stats stayed at 0 rows.
                                .withIdleness(Duration.ofSeconds(30)));

        KeyedStream<TransactionEvent, Integer> keyed = events.keyBy(e -> e.accountId);

        SingleOutputStreamOperator<BalanceUpdate> balances = keyed.process(new BalanceAndAnomalyFunction()).name("balance-and-anomaly");
        DataStream<AnomalyFlag> anomalies = balances.getSideOutput(BalanceAndAnomalyFunction.ANOMALY_TAG);

        DataStream<WindowedStats> windowedStats = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new WindowStatsFunction())
                .name("windowed-stats");

        balances.addSink(JdbcSink.sink(
                "INSERT INTO balance_snapshot (account_id, balance, updated_at) VALUES (?, ?, ?)",
                (statement, b) -> {
                    statement.setInt(1, b.accountId);
                    statement.setBigDecimal(2, b.balance);
                    statement.setTimestamp(3, Timestamp.from(b.updatedAt));
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions()
        )).name("balance-snapshot-sink");

        windowedStats.addSink(JdbcSink.sink(
                "INSERT INTO windowed_txn_stats (window_start, account_id, txn_count, txn_amount) VALUES (?, ?, ?, ?)",
                (statement, s) -> {
                    statement.setTimestamp(1, Timestamp.from(s.windowStart));
                    statement.setInt(2, s.accountId);
                    statement.setLong(3, s.txnCount);
                    statement.setBigDecimal(4, s.txnAmount);
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions()
        )).name("windowed-stats-sink");

        anomalies.addSink(JdbcSink.sink(
                "INSERT INTO anomaly_flags (account_id, reason, detected_at) VALUES (?, ?, ?)",
                (statement, a) -> {
                    statement.setInt(1, a.accountId);
                    statement.setString(2, a.reason);
                    statement.setTimestamp(3, Timestamp.from(a.detectedAt));
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions()
        )).name("anomaly-flags-sink");

        env.execute("realtime-txn-pipeline");
    }

    private static JdbcExecutionOptions jdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(50)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build();
    }

    private static JdbcConnectionOptions jdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(CLICKHOUSE_URL)
                .withDriverName(CLICKHOUSE_DRIVER)
                .withUsername("default")
                .withPassword("")
                .build();
    }
}
