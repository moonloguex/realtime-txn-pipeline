package com.jm.txnpipeline.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Parses Debezium's schema-less JSON change event and emits only insert ("c") events
 * with a non-null "after" payload — this pipeline only cares about new transactions.
 */
public class DebeziumEventParser implements FlatMapFunction<String, TransactionEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<TransactionEvent> out) throws Exception {
        JsonNode root = MAPPER.readTree(value);
        JsonNode after = root.get("after");
        if (after == null || after.isNull()) {
            return;
        }
        int accountId = after.get("account_id").asInt();
        BigDecimal amount = new BigDecimal(after.get("amount").asText());
        Instant createdAt = Instant.parse(after.get("created_at").asText());
        out.collect(new TransactionEvent(accountId, amount, createdAt));
    }
}
