package com.jm.txnpipeline.flink;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DebeziumEventParserTest {

    private static List<TransactionEvent> parse(String json) throws Exception {
        List<TransactionEvent> out = new ArrayList<>();
        new DebeziumEventParser().flatMap(json, new Collector<>() {
            @Override
            public void collect(TransactionEvent record) {
                out.add(record);
            }

            @Override
            public void close() {
            }
        });
        return out;
    }

    private static String event(String op, String after) {
        return "{\"before\":null,\"after\":" + after + ",\"op\":\"" + op + "\",\"ts_ms\":1}";
    }

    private static final String ROW =
            "{\"id\":7,\"account_id\":3,\"amount\":\"-25810.00\",\"created_at\":\"2026-09-24T04:31:02.123456Z\"}";

    @Test
    void insertIsParsedWithExactDecimalAmount() throws Exception {
        List<TransactionEvent> out = parse(event("c", ROW));

        assertEquals(1, out.size());
        assertEquals(3, out.get(0).accountId);
        // decimal.handling.mode=string: amount must round-trip exactly, scale included
        assertEquals(new BigDecimal("-25810.00"), out.get(0).amount);
        assertEquals(Instant.parse("2026-09-24T04:31:02.123456Z"), out.get(0).createdAt);
    }

    @Test
    void snapshotReadsUpdatesAndDeletesAreSkipped() throws Exception {
        // "r" re-emitted on a snapshot re-run would double-count every existing transaction
        assertEquals(0, parse(event("r", ROW)).size());
        assertEquals(0, parse(event("u", ROW)).size());
        assertEquals(0, parse(event("d", "null")).size());
    }

    @Test
    void tombstoneLikeEventsAreSkipped() throws Exception {
        assertEquals(0, parse(event("c", "null")).size());
        assertEquals(0, parse("{\"after\":" + ROW + "}").size()); // no op field
    }
}
