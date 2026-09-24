package com.jm.txnpipeline.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BalanceAndAnomalyFunctionTest {

    private static final Instant T0 = Instant.parse("2026-09-24T00:00:00Z");

    private KeyedOneInputStreamOperatorTestHarness<Integer, TransactionEvent, BalanceUpdate> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = newHarness();
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, TransactionEvent, BalanceUpdate> newHarness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new BalanceAndAnomalyFunction()), e -> e.accountId, Types.INT);
    }

    private static void send(KeyedOneInputStreamOperatorTestHarness<Integer, TransactionEvent, BalanceUpdate> h,
                             int account, String amount, long offsetMs) throws Exception {
        Instant at = T0.plusMillis(offsetMs);
        h.processElement(new StreamRecord<>(new TransactionEvent(account, new BigDecimal(amount), at), at.toEpochMilli()));
    }

    private List<BigDecimal> balances() {
        return harness.extractOutputValues().stream().map(b -> b.balance).collect(Collectors.toList());
    }

    private List<String> anomalies() {
        // harness returns null (not empty) until the first side-output record
        var side = harness.getSideOutput(BalanceAndAnomalyFunction.ANOMALY_TAG);
        if (side == null) {
            return List.of();
        }
        return side.stream()
                .map(r -> r.getValue().accountId + ":" + r.getValue().reason)
                .collect(Collectors.toList());
    }

    @Test
    void runningBalanceIsExactAndPerAccount() throws Exception {
        send(harness, 1, "100.10", 0);
        send(harness, 2, "-5.00", 10_000);
        send(harness, 1, "0.20", 20_000);
        send(harness, 1, "-300.30", 30_000);

        // BigDecimal, not double: 100.10 + 0.20 must be exactly 100.30
        assertEquals(List.of(new BigDecimal("100.10"), new BigDecimal("-5.00"),
                new BigDecimal("100.30"), new BigDecimal("-200.00")), balances());
    }

    @Test
    void balanceSurvivesCheckpointRestore() throws Exception {
        send(harness, 1, "1000.00", 0);
        send(harness, 2, "50.00", 10_000);
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        send(harness, 1, "999.00", 20_000); // lost with the "crashed" task, replayed from Kafka after restore
        harness.close();

        harness = newHarness();
        harness.initializeState(snapshot);
        harness.open();
        send(harness, 1, "999.00", 20_000);
        send(harness, 2, "-25.00", 30_000);

        // restored state + replay = same result as an uninterrupted run, no double-counting
        assertEquals(List.of(new BigDecimal("1999.00"), new BigDecimal("25.00")), balances());
    }

    @Test
    void largeAmountThresholdIsStrictAndSignAgnostic() throws Exception {
        send(harness, 1, "47500.00", 0);      // at threshold: not flagged
        send(harness, 2, "47500.01", 10_000); // just above
        send(harness, 3, "-47500.01", 20_000); // withdrawals count too

        assertEquals(List.of("2:large_amount", "3:large_amount"), anomalies());
    }

    @Test
    void highVelocityNeedsFourTxnsWithinFiveSeconds() throws Exception {
        send(harness, 1, "1", 0);
        send(harness, 1, "1", 1_000);
        send(harness, 1, "1", 2_000);
        assertEquals(List.of(), anomalies());

        send(harness, 1, "1", 5_000); // 4th txn, first one exactly 5s ago: window is inclusive
        assertEquals(List.of("1:high_velocity"), anomalies());
    }

    @Test
    void highVelocityIgnoresOldTxnsAndOtherAccounts() throws Exception {
        send(harness, 1, "1", 0);
        send(harness, 1, "1", 1_000);
        send(harness, 2, "1", 1_500);  // different key, must not count toward account 1
        send(harness, 1, "1", 2_000);
        send(harness, 1, "1", 5_001);  // 0ms txn has aged out: only 3 in window

        assertEquals(List.of(), anomalies());
    }
}
