-- ClickHouse schema for the real-time transaction pipeline.
-- Flink writes here via JDBC sink (see TransactionProcessor.java); Grafana reads from here.

-- account_id is Int32 everywhere: Postgres accounts.id is SERIAL (4-byte int) and
-- Flink's TransactionEvent.accountId/BalanceUpdate.accountId/AnomalyFlag.accountId are
-- Java `int` — String would only add conversion cost with no benefit. If account IDs ever
-- move to UUIDs, revisit this.

-- Per-account latest balance. Query pattern: "current balance for account X" / "latest
-- balance for every account" (Grafana stat panel + time series).
-- ReplacingMergeTree(updated_at) keeps only the newest row per account_id, but merges are
-- async — duplicate rows can exist between merges. EVERY read must use FINAL or
-- argMax(balance, updated_at) GROUP BY account_id, e.g.:
--   SELECT account_id, argMax(balance, updated_at) AS balance FROM balance_snapshot GROUP BY account_id
-- Deliberately NOT partitioned: ReplacingMergeTree only dedupes within a single partition,
-- so partitioning by month would let stale rows for the same account survive forever once
-- balance updates cross a month boundary.
CREATE TABLE IF NOT EXISTS balance_snapshot (
    account_id Int32,
    balance Decimal(18,2),
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id;

-- 1-minute windowed aggregates. Query pattern: per-account volume/amount time series over a
-- Grafana time range ($__timeFilter(window_start)).
-- ReplacingMergeTree() (no version column) on (account_id, window_start): the JDBC sink is
-- structurally at-least-once (see TransactionProcessor.java jdbcConnectionOptions /
-- withMaxRetries(3)), and a jdbc-v2 0.8.6 driver bug (PreparedStatement batch never cleared
-- after executeBatch(), so every flush resent all earlier rows) has already produced real
-- duplicate INSERTs of the exact same (account_id, window_start) row in production —
-- confirmed identical txn_count/txn_amount across the duplicates, never divergent values. A
-- version column is unnecessary because "keep whichever duplicate merges last" is safe when
-- duplicates are guaranteed byte-identical; a plain WindowStatsFunction re-emit for the same
-- window always produces the same aggregate. That driver bug is fixed upstream
-- (clickhouse-java#2548, 0.9.2+; the job now uses 0.9.8), but this is the schema-level
-- backstop for any future at-least-once retry.
-- EVERY read must dedupe: merges are async, so duplicate rows can exist between merges.
-- Use FINAL, e.g.:
--   SELECT window_start, sum(txn_amount) FROM windowed_txn_stats FINAL WHERE $__timeFilter(window_start) GROUP BY window_start
-- argMax(..., <version>) is NOT an option here since there is no version column to compare on.
CREATE TABLE IF NOT EXISTS windowed_txn_stats (
    window_start DateTime64(3),
    account_id Int32,
    txn_count UInt32,
    txn_amount Decimal(18,2)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (account_id, window_start);

-- Anomaly flags. Query pattern: recent-alerts list (all accounts) and per-account alert
-- history — the latter needs account_id first in the sort key or it's a full scan.
-- ReplacingMergeTree() (no version column) on (account_id, reason, detected_at) — same
-- rationale as windowed_txn_stats above: the same driver batch-not-cleared bug produced real
-- duplicate INSERTs here too, and (account_id, reason, detected_at) IS the entire row (no
-- other columns exist to diverge), so any duplicate is by construction identical. No version
-- column needed.
-- EVERY read must dedupe: use FINAL, e.g.:
--   SELECT account_id, reason, detected_at FROM anomaly_flags FINAL ORDER BY detected_at DESC LIMIT 50
--   SELECT * FROM anomaly_flags FINAL WHERE account_id = {account_id} ORDER BY detected_at DESC
CREATE TABLE IF NOT EXISTS anomaly_flags (
    account_id Int32,
    reason String,
    detected_at DateTime64(3)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (account_id, detected_at);
