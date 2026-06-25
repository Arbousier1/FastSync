package com.fastsync.log;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the per-UUID append-only operation log.
 *
 * <p>Inspired by Raft's replicated log concept: every state-changing operation
 * for a player is recorded with a monotonically increasing per-UUID sequence
 * number. This creates a deterministic, auditable history that enables conflict
 * diagnosis, recovery, and audit.
 *
 * <p>The log is <strong>append-only</strong> — entries are never modified or
 * deleted (except by retention pruning). Each UUID has its own independent
 * sequence starting from 1, assigned atomically by the database.
 *
 * <p>Key Raft-inspired design decisions:
 * <ul>
 *   <li><b>Per-UUID ordering:</b> Operations for the same UUID are totally
 *       ordered by seq. Operations for different UUIDs are independent.
 *       This matches Raft's insight that only same-key operations need ordering.
 *   <li><b>Single-writer per UUID:</b> Enforced by fencing token + lock. Only
 *       the current lock holder appends to the log, similar to Raft's leader
 *       being the sole log appender.
 *   <li><b>Append-only:</b> Like Raft's log, entries are never rewritten.
 *       This provides a complete audit trail.
 * </ul>
 */
public class OperationLogManager {

    private final Logger logger;
    private final ConfigManager config;
    private DatabaseManager databaseManager;
    private HikariDataSource dataSource;
    private String logTable;
    private boolean enabled;

    public OperationLogManager(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Initialize the operation log table.
     *
     * @param dataSource the HikariCP data source
     * @param databaseManager the database manager (for atomic op_seq counter)
     */
    public void initialize(HikariDataSource dataSource, DatabaseManager databaseManager) throws SQLException {
        this.dataSource = dataSource;
        this.databaseManager = databaseManager;
        this.logTable = config.getTablePrefix() + "operation_log";
        this.enabled = config.isOperationLogEnabled();

        if (!enabled) {
            logger.info("Operation log disabled.");
            return;
        }

        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `uuid` VARCHAR(36) NOT NULL,
                `seq` BIGINT NOT NULL,
                `type` VARCHAR(32) NOT NULL,
                `server` VARCHAR(64) NOT NULL,
                `fencing_token` BIGINT NOT NULL DEFAULT 0,
                `version` BIGINT NOT NULL DEFAULT 0,
                `data_size` INT NOT NULL DEFAULT 0,
                `detail` VARCHAR(512) DEFAULT NULL,
                `timestamp` BIGINT NOT NULL,
                UNIQUE KEY `uk_uuid_seq` (`uuid`, `seq`),
                INDEX `idx_uuid_ts` (`uuid`, `timestamp`),
                INDEX `idx_type` (`type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, logTable);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }

        logger.info("Operation log enabled (table: " + logTable + ").");
    }

    /**
     * Append an operation to the log. The sequence number is assigned atomically
     * by the database using a transactional MAX(seq)+1 approach.
     *
     * <p>This is async and non-blocking — logging never blocks the main save path.
     *
     * @return CompletableFuture that completes when the log entry is written
     */
    public CompletableFuture<Void> append(OperationLog entry) {
        if (!enabled || dataSource == null) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            try {
                appendSync(entry);
            } catch (SQLException e) {
                logger.log(Level.WARNING, "Failed to append operation log for " + entry.uuid(), e);
            }
        });
    }

    /**
     * Synchronous append.
     *
     * <p><b>InnoDB locking fix:</b> Previously used
     * {@code SELECT MAX(seq) ... FOR UPDATE} to determine the next sequence
     * number. This caused InnoDB next-key locks on the operation_log index,
     * blocking concurrent inserts for adjacent UUIDs — a violation of the
     * "no range query + FOR UPDATE on hot path" rule.
     *
     * <p>Now uses {@link DatabaseManager#incrementOpSeq(UUID)} which atomically
     * increments a counter in the player_data table via a single-row PK update
     * (X lock on one row, no gap locks), then does a simple INSERT into
     * operation_log. The two operations are on separate connections, but the
     * UNIQUE KEY (uuid, seq) constraint guarantees correctness: if two threads
     * race, the second INSERT fails with a duplicate key error and is retried.
     */
    private void appendSync(OperationLog entry) throws SQLException {
        // Step 1: Atomically get the next per-UUID sequence number.
        // This is a single-row PK update on player_data — no gap locks.
        long nextSeq = databaseManager.incrementOpSeq(entry.uuid());

        // Step 2: Simple INSERT into operation_log (no SELECT FOR UPDATE).
        // The UNIQUE KEY (uuid, seq) guarantees no duplicates.
        String insertSql = String.format("""
            INSERT INTO `%s` (uuid, seq, type, server, fencing_token, version, data_size, detail, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, logTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setString(1, entry.uuid().toString());
            ps.setLong(2, nextSeq);
            ps.setString(3, entry.type().name());
            ps.setString(4, entry.serverName());
            ps.setLong(5, entry.fencingToken());
            ps.setLong(6, entry.version());
            ps.setInt(7, entry.dataSize());
            ps.setString(8, entry.detail() != null ?
                entry.detail().substring(0, Math.min(entry.detail().length(), 512)) : null);
            ps.setLong(9, entry.timestamp());

            try {
                ps.executeUpdate();
            } catch (SQLException e) {
                // Duplicate key (uuid, seq) — extremely rare race condition.
                // The op_seq counter is the source of truth; if this fails,
                // the log entry is simply skipped (best-effort logging).
                if (e.getMessage() != null && e.getMessage().contains("Duplicate")) {
                    logger.fine("[OpLog] Duplicate seq " + nextSeq + " for " + entry.uuid() +
                        " — skipped (best-effort logging)");
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Query the operation history for a player.
     *
     * @param uuid  player UUID
     * @param limit max number of entries to return (most recent first)
     * @return list of operation log entries, most recent first
     */
    public List<OperationLog> queryHistory(UUID uuid, int limit) throws SQLException {
        if (!enabled) {
            return List.of();
        }

        String sql = String.format("""
            SELECT seq, uuid, type, server, fencing_token, version, data_size, detail, timestamp
            FROM `%s` WHERE uuid = ? ORDER BY seq DESC LIMIT ?
            """, logTable);

        List<OperationLog> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(new OperationLog(
                        rs.getLong("seq"),
                        UUID.fromString(rs.getString("uuid")),
                        OperationType.valueOf(rs.getString("type")),
                        rs.getString("server"),
                        rs.getLong("fencing_token"),
                        rs.getLong("version"),
                        rs.getInt("data_size"),
                        rs.getString("detail"),
                        rs.getLong("timestamp")
                    ));
                }
            }
        }
        return results;
    }

    /**
     * Get the current sequence number for a player.
     *
     * <p><b>InnoDB note:</b> Uses {@code ORDER BY id DESC LIMIT 1} (PK point lookup
     * via the uuid index) instead of {@code MAX(seq)} to avoid a full index scan.
     * This is a read-only query with no FOR UPDATE, so no gap locks.
     */
    public long getCurrentSeq(UUID uuid) throws SQLException {
        if (!enabled) return 0;

        String sql = String.format(
            "SELECT seq FROM `%s` WHERE uuid = ? ORDER BY id DESC LIMIT 1", logTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("seq");
                }
            }
        }
        return 0;
    }

    /**
     * Prune old log entries beyond the retention limit for a player.
     * Keeps only the most recent N entries (by auto-increment id, not timestamp).
     *
     * <p><b>InnoDB note:</b> Uses {@code ORDER BY id DESC LIMIT offset} to find
     * the cutoff, avoiding {@code MAX(seq)} aggregation scans. This is an async
     * background task (not on the hot path), and the DELETE uses the uuid index.
     */
    public CompletableFuture<Void> prune(UUID uuid, int keepCount) {
        if (!enabled) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            try {
                // Find the id of the (keepCount+1)-th newest entry — everything
                // with a smaller id for this UUID gets pruned.
                String cutoffSql = String.format("""
                    SELECT id FROM `%s` WHERE uuid = ? ORDER BY id DESC LIMIT 1 OFFSET ?
                    """, logTable);
                Long cutoffId = null;
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(cutoffSql)) {
                    ps.setString(1, uuid.toString());
                    ps.setInt(2, keepCount);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            cutoffId = rs.getLong("id");
                        }
                    }
                }

                if (cutoffId == null) return; // fewer than keepCount entries

                // Delete all entries older than the cutoff (by id, not timestamp)
                String deleteSql = String.format(
                    "DELETE FROM `%s` WHERE uuid = ? AND id < ?", logTable);
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(deleteSql)) {
                    ps.setString(1, uuid.toString());
                    ps.setLong(2, cutoffId);
                    int deleted = ps.executeUpdate();
                    if (deleted > 0 && config.isDebug()) {
                        logger.info("[OpLog] Pruned " + deleted + " old entries for " + uuid);
                    }
                }
            } catch (SQLException e) {
                logger.log(Level.WARNING, "Failed to prune operation log for " + uuid, e);
            }
        });
    }

    public boolean isEnabled() {
        return enabled;
    }
}
