package com.fastsync.sync;

import com.fastsync.config.ConfigManager;
import com.fastsync.log.FileOperationLogManager;
import com.fastsync.log.OperationLog;
import com.fastsync.log.OperationType;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-UUID operation log (Raft-inspired) management.
 *
 * <p>Extracted from {@link SyncManager} to reduce its class size. Wraps
 * {@link FileOperationLogManager} and provides null-safe accessors for
 * the command layer ({@code /fastsync status}). The {@link #logOperation}
 * method is non-blocking — logging never blocks the main save/load path.
 */
public class OperationLogDelegate {

    private final Logger logger;
    private final ConfigManager config;
    private FileOperationLogManager operationLogManager;

    public OperationLogDelegate(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Initialize the file-based operation log.
     *
     * @param dataFolder the plugin data folder
     */
    public void initialize(java.io.File dataFolder) {
        try {
            operationLogManager = new FileOperationLogManager(
                dataFolder.toPath(), config.getOperationLogRetention());
            operationLogManager.initialize();
            logger.info("Operation log enabled (file-based, retention=" +
                config.getOperationLogRetention() + " entries per UUID)");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to initialize operation log — continuing without audit trail", e);
            operationLogManager = null;
        }
    }

    /** Close the operation log manager. */
    public void close() {
        if (operationLogManager != null) {
            operationLogManager.close();
            operationLogManager = null;
        }
    }

    // ==================== Telemetry getters ====================

    public boolean isEnabled() {
        return operationLogManager != null && operationLogManager.isEnabled();
    }

    public int getQueueSize() {
        return operationLogManager != null ? operationLogManager.getQueueSize() : -1;
    }

    public int getQueueCapacity() {
        return operationLogManager != null ? operationLogManager.getQueueCapacity() : -1;
    }

    public long getDroppedTotal() {
        return operationLogManager != null ? operationLogManager.getDroppedCount() : 0L;
    }

    public long getLastDropAt() {
        return operationLogManager != null ? operationLogManager.getLastDropTimestamp() : 0L;
    }

    // ==================== Logging ====================

    /**
     * Log an operation to the per-UUID operation log.
     * Non-blocking — logging never blocks the main save/load path.
     *
     * @param uuid        player UUID
     * @param type        operation type
     * @param fencingToken fencing token at time of operation
     * @param version     data version
     * @param dataSize    compressed data size in bytes
     * @param detail      human-readable detail
     */
    public void logOperation(UUID uuid, OperationType type, long fencingToken,
                              long version, int dataSize, String detail) {
        if (operationLogManager != null) {
            OperationLog log = OperationLog.create(uuid, type, config.getServerName(),
                fencingToken, version, dataSize, detail);
            operationLogManager.append(log)
                .thenRun(() -> operationLogManager.prune(uuid, config.getOperationLogRetention()))
                .exceptionally(e -> {
                    logger.log(Level.WARNING, "[OpLog] Failed to log " + type + " for " + uuid, e);
                    return null;
                });
        }
    }

    /**
     * Query the operation history for a player.
     *
     * @param uuid  player UUID
     * @param limit maximum number of entries
     * @return list of operation log entries, empty if unavailable
     */
    public List<OperationLog> queryHistory(UUID uuid, int limit) {
        if (operationLogManager == null) {
            return List.of();
        }
        try {
            return operationLogManager.queryHistory(uuid, limit);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to query operation log for " + uuid, e);
            return List.of();
        }
    }
}
