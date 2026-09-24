package com.jm.txnpipeline.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WindowStatsFunctionTest {

    private static final Instant T0 = Instant.parse("2026-09-24T00:00:00Z");

    private static TransactionEvent txn(int account, String amount, long offsetMs) {
        return new TransactionEvent(account, new BigDecimal(amount), T0.plusMillis(offsetMs));
    }

    @Test
    void oneMinuteTumblingWindowsPerAccount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<String> result = env
                .fromData(
                        txn(1, "10.50", 0),
                        txn(1, "-0.50", 59_999),   // last ms of window 1
                        txn(2, "7.00", 30_000),
                        txn(1, "3.00", 60_000),    // first ms of window 2
                        txn(1, "4.00", 58_000))    // out of order, within the 5s bound
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.createdAt.toEpochMilli()))
                .keyBy(e -> e.accountId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new WindowStatsFunction())
                .executeAndCollect(10)
                .stream()
                .map(s -> s.accountId + "@" + s.windowStart + " n=" + s.txnCount + " sum=" + s.txnAmount)
                .sorted()
                .collect(Collectors.toList());

        assertEquals(List.of(
                "1@2026-09-24T00:00:00Z n=3 sum=14.00",
                "1@2026-09-24T00:01:00Z n=1 sum=3.00",
                "2@2026-09-24T00:00:00Z n=1 sum=7.00"), result);
    }
}
