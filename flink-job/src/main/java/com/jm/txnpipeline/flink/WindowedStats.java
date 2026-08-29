package com.jm.txnpipeline.flink;

import java.math.BigDecimal;
import java.time.Instant;

public class WindowedStats {
    public Instant windowStart;
    public int accountId;
    public long txnCount;
    public BigDecimal txnAmount;

    public WindowedStats() {
    }

    public WindowedStats(Instant windowStart, int accountId, long txnCount, BigDecimal txnAmount) {
        this.windowStart = windowStart;
        this.accountId = accountId;
        this.txnCount = txnCount;
        this.txnAmount = txnAmount;
    }
}
