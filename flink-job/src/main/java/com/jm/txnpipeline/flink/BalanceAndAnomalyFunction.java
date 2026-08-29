package com.jm.txnpipeline.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Per account: maintains a running balance (keyed state) and flags anomalies via a side output.
 * Two simple anomaly rules: a single transaction over a fixed amount, or too many transactions
 * in a short rolling window ("velocity").
 */
public class BalanceAndAnomalyFunction extends KeyedProcessFunction<Integer, TransactionEvent, BalanceUpdate> {

    public static final OutputTag<AnomalyFlag> ANOMALY_TAG = new OutputTag<AnomalyFlag>("anomaly") {
    };

    // amount ~ Uniform(-50000, 50000); threshold tuned so "large_amount" fires on ~5% of txns
    private static final BigDecimal AMOUNT_THRESHOLD = new BigDecimal("47500");
    // generator emits every 0.5-2s across 5 accounts (~2.5-10s per account); tuned so a burst,
    // not steady traffic, is what trips "high_velocity"
    private static final int VELOCITY_COUNT_THRESHOLD = 4;
    private static final long VELOCITY_WINDOW_MS = 5_000;

    private transient ValueState<BigDecimal> balanceState;
    private transient ListState<Long> recentTxnTimestamps;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        balanceState = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", BigDecimal.class));
        recentTxnTimestamps = getRuntimeContext().getListState(new ListStateDescriptor<>("recentTxnTimestamps", Long.class));
    }

    @Override
    public void processElement(TransactionEvent event, Context ctx, Collector<BalanceUpdate> out) throws Exception {
        BigDecimal balance = balanceState.value();
        balance = (balance == null ? BigDecimal.ZERO : balance).add(event.amount);
        balanceState.update(balance);
        out.collect(new BalanceUpdate(event.accountId, balance, event.createdAt));

        if (event.amount.abs().compareTo(AMOUNT_THRESHOLD) > 0) {
            ctx.output(ANOMALY_TAG, new AnomalyFlag(event.accountId, "large_amount", event.createdAt));
        }

        long now = event.createdAt.toEpochMilli();
        List<Long> timestamps = new ArrayList<>();
        for (Long t : recentTxnTimestamps.get()) {
            if (now - t <= VELOCITY_WINDOW_MS) {
                timestamps.add(t);
            }
        }
        timestamps.add(now);
        recentTxnTimestamps.update(timestamps);

        if (timestamps.size() >= VELOCITY_COUNT_THRESHOLD) {
            ctx.output(ANOMALY_TAG, new AnomalyFlag(event.accountId, "high_velocity", Instant.ofEpochMilli(now)));
        }
    }
}
