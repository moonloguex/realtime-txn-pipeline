CREATE TABLE IF NOT EXISTS balance_snapshot (
    account_id String,
    balance Decimal(18,2),
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS windowed_txn_stats (
    window_start DateTime64(3),
    account_id String,
    txn_count UInt32,
    txn_amount Decimal(18,2)
) ENGINE = MergeTree
ORDER BY (account_id, window_start);

CREATE TABLE IF NOT EXISTS anomaly_flags (
    account_id String,
    reason String,
    detected_at DateTime64(3)
) ENGINE = MergeTree
ORDER BY detected_at;
