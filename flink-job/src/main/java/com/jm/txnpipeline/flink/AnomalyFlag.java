package com.jm.txnpipeline.flink;

import java.time.Instant;

public class AnomalyFlag {
    public int accountId;
    public String reason;
    public Instant detectedAt;

    public AnomalyFlag() {
    }

    public AnomalyFlag(int accountId, String reason, Instant detectedAt) {
        this.accountId = accountId;
        this.reason = reason;
        this.detectedAt = detectedAt;
    }
}
