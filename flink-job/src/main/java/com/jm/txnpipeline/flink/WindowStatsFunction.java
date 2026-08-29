package com.jm.txnpipeline.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Instant;

public class WindowStatsFunction extends ProcessWindowFunction<TransactionEvent, WindowedStats, Integer, TimeWindow> {

    @Override
    public void process(Integer accountId, Context context, Iterable<TransactionEvent> elements, Collector<WindowedStats> out) {
        long count = 0;
        BigDecimal sum = BigDecimal.ZERO;
        for (TransactionEvent e : elements) {
            count++;
            sum = sum.add(e.amount);
        }
        out.collect(new WindowedStats(Instant.ofEpochMilli(context.window().getStart()), accountId, count, sum));
    }
}
