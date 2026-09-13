package com.jm.txnpipeline.flink;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Drop-in replacement for {@code JdbcBatchStatementExecutor.simple(...)} that re-prepares its
 * PreparedStatement after every executeBatch() instead of reusing one for the sink's lifetime.
 *
 * Root cause this works around: com.clickhouse:jdbc-v2:0.8.6's PreparedStatementImpl (the driver
 * actually used at runtime by "jdbc:clickhouse:..." URLs - see the CLICKHOUSE_URL comment in
 * TransactionProcessor) has a confirmed bug in its literal-VALUES batch-insert fast path
 * (triggered by any "INSERT INTO t (...) VALUES (?, ?, ?)" statement, exactly what this job uses):
 * PreparedStatementImpl.addBatch() appends each row to a private `batchValues` list, and
 * executeInsertBatch() concatenates and sends that whole list on executeBatch() - but never
 * clears it afterward, and PreparedStatementImpl does not override clearBatch() either (the
 * inherited StatementImpl.clearBatch() only clears an unrelated field used by the non-VALUES
 * path). Flink's flink-connector-jdbc SimpleBatchStatementExecutor reuses one PreparedStatement
 * across every flush for the sink task's whole lifetime (by design, for efficiency), relying on
 * the JDBC contract that executeBatch() clears its own batch - a contract this driver violates.
 * The result: every flush resends every row ever added to that PreparedStatement, not just the
 * ones added since the last flush - confirmed live by seeing every windowed_txn_stats/anomaly_flags
 * row from early flushes re-inserted (with byte-identical values) on every later flush, with the
 * duplicate count growing by exactly one on each subsequent flush interval and per checkpoint
 * (GenericJdbcSinkFunction.snapshotState() calls flush() too), never involving a logged
 * SQLException/retry - this is what earlier investigation misread as a connection-reuse/CLOSE_WAIT
 * race, since older rows appeared to accumulate duplicates in proportion to their age.
 *
 * Re-preparing the statement after every executeBatch() guarantees the driver starts each flush
 * with a fresh, empty internal batch, regardless of whether it clears its own state. This has to
 * live in application code because flink-connector-jdbc's public API has no hook to force
 * re-preparation on a normal (non-error) flush - only on a retry after a thrown SQLException
 * (JdbcOutputFormat.updateExecutor).
 */
public class ReconnectSafeBatchStatementExecutor<T> implements JdbcBatchStatementExecutor<T> {

    private final String sql;
    private final JdbcStatementBuilder<T> parameterSetter;
    private final List<T> batch;

    private transient Connection connection;
    private transient PreparedStatement st;

    public ReconnectSafeBatchStatementExecutor(String sql, JdbcStatementBuilder<T> statementBuilder) {
        this.sql = sql;
        this.parameterSetter = statementBuilder;
        this.batch = new ArrayList<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
        this.st = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(T record) {
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            for (T r : batch) {
                parameterSetter.accept(st, r);
                st.addBatch();
            }
            st.executeBatch();
            batch.clear();
            st.close();
            st = connection.prepareStatement(sql);
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}
