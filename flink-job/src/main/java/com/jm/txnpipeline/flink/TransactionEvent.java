package com.jm.txnpipeline.flink;

import java.math.BigDecimal;
import java.time.Instant;

public class TransactionEvent {
    public int accountId;
    public BigDecimal amount;
    public Instant createdAt;

    public TransactionEvent() {
    }

    public TransactionEvent(int accountId, BigDecimal amount, Instant createdAt) {
        this.accountId = accountId;
        this.amount = amount;
        this.createdAt = createdAt;
    }
}
