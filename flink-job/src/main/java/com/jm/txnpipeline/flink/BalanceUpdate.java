package com.jm.txnpipeline.flink;

import java.math.BigDecimal;
import java.time.Instant;

public class BalanceUpdate {
    public int accountId;
    public BigDecimal balance;
    public Instant updatedAt;

    public BalanceUpdate() {
    }

    public BalanceUpdate(int accountId, BigDecimal balance, Instant updatedAt) {
        this.accountId = accountId;
        this.balance = balance;
        this.updatedAt = updatedAt;
    }
}
