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
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId("txn-flink-processor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TransactionEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "cdc-source")
                .flatMap(new DebeziumEventParser())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.createdAt.toEpochMilli()));

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
                    statement.setString(1, String.valueOf(b.accountId));
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
                    statement.setString(2, String.valueOf(s.accountId));
                    statement.setLong(3, s.txnCount);
                    statement.setBigDecimal(4, s.txnAmount);
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions()
        )).name("windowed-stats-sink");

        anomalies.addSink(JdbcSink.sink(
                "INSERT INTO anomaly_flags (account_id, reason, detected_at) VALUES (?, ?, ?)",
                (statement, a) -> {
                    statement.setString(1, String.valueOf(a.accountId));
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
