package com.fastsync.sync;

import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.spool.EncodedFinalSave;
import com.fastsync.spool.FinalSaveEncoder;
import com.fastsync.spool.FinalSaveSpool;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Telemetry and spool management for final-save (QUIT/SHUTDOWN) operations.
 *
 * <p>Extracted from {@link SyncManager} to reduce its class size. Owns the
 * saturation counters, spool enqueue/reject tracking, and the spool-write
 * methods. All public getters are delegated back through SyncManager for
 * API compatibility with the command layer ({@code /fastsync status}).
 *
 * <p>Counters are thread-safe ({@link AtomicLong}) because final-save
 * submissions happen on multiple threads (entity thread for collect,
 * final-save executor for the actual DB write).
 */
public class FinalSaveStats {

    private final Logger logger;
    private final ConfigManager config;

    // Final-save saturation telemetry. A queue-full event on finalSaveExecutor
    // immediately causes a synchronous fallback today, so we count both the
    // rejection and the fallback and surface them in /fastsync status.
    private final AtomicLong queueFullTotal = new AtomicLong();
    private final AtomicLong lastQueueFullAt = new AtomicLong();
    private final AtomicLong spoolEnqueuedTotal = new AtomicLong();
    private final AtomicLong lastSpoolEnqueuedAt = new AtomicLong();
    private final AtomicLong spoolRejectedTotal = new AtomicLong();
    private final AtomicLong lastSpoolRejectedAt = new AtomicLong();
    private final AtomicLong syncFallbackTotal = new AtomicLong();
    private final AtomicLong lastSyncFallbackAt = new AtomicLong();

    private FinalSaveSpool spool;

    /** Outcome when the final-save executor rejects a task. */
    public enum QueueFullOutcome {
        SPOOLED, SPOOL_UNAVAILABLE, SPOOL_FAILED, SYNC_FALLBACK
    }

    /** Result of attempting to spool a final save directly (queue-full path). */
    public record SpoolResult(QueueFullOutcome outcome, String detail) {}

    public FinalSaveStats(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /** Called by SyncManager.initialize() after the spool is created. */
    void setSpool(FinalSaveSpool spool) {
        this.spool = spool;
    }

    FinalSaveSpool getSpool() {
        return spool;
    }

    // ==================== Telemetry getters ====================

    public long getQueueFullTotal() {
        return queueFullTotal.get();
    }

    public long getLastQueueFullAt() {
        return lastQueueFullAt.get();
    }

    public long getSpoolEnqueuedTotal() {
        return spoolEnqueuedTotal.get();
    }

    public long getLastSpoolEnqueuedAt() {
        return lastSpoolEnqueuedAt.get();
    }

    public long getSpoolRejectedTotal() {
        return spoolRejectedTotal.get();
    }

    public long getLastSpoolRejectedAt() {
        return lastSpoolRejectedAt.get();
    }

    public long getSyncFallbackTotal() {
        return syncFallbackTotal.get();
    }

    public long getLastSyncFallbackAt() {
        return lastSyncFallbackAt.get();
    }

    public boolean hasAlert() {
        return syncFallbackTotal.get() > 0
            || spoolRejectedTotal.get() > 0
            || getSpoolFailedCount() > 0;
    }

    public boolean hasWarning() {
        return queueFullTotal.get() > 0
            || spoolEnqueuedTotal.get() > 0
            || getSpoolPendingCount() > 0;
    }

    // ==================== Spool telemetry ====================

    public long getSpoolPendingCount() {
        return spool != null ? spool.getPendingCount() : 0;
    }

    public long getSpoolFailedCount() {
        return spool != null ? spool.getFailedCount() : 0;
    }

    public long getSpoolBytes() {
        return spool != null ? spool.getTotalBytes() : 0;
    }

    public long getSpoolLastReplayAt() {
        return spool != null ? spool.getLastReplayAt() : 0;
    }

    public String getSpoolLastError() {
        return spool != null ? spool.getLastError() : null;
    }

    // ==================== Record queue-full event ====================

    /**
     * Record a final-save executor queue-full event, incrementing the
     * appropriate counters based on the outcome.
     *
     * @param kind     save kind label (e.g. "QUIT", "SHUTDOWN")
     * @param uuid     player UUID
     * @param detail   human-readable detail message
     * @param cause    the original RejectedExecutionException
     * @param outcome  what happened after the queue was full
     */
    public void recordQueueFull(
            String kind, UUID uuid, String detail,
            RejectedExecutionException cause, QueueFullOutcome outcome) {
        long now = System.currentTimeMillis();
        long queueFullCount = queueFullTotal.incrementAndGet();
        lastQueueFullAt.set(now);
        long spooled = spoolEnqueuedTotal.get();
        long spoolRejected = spoolRejectedTotal.get();
        long syncFallback = syncFallbackTotal.get();
        switch (outcome) {
            case SPOOLED -> {
                spooled = spoolEnqueuedTotal.incrementAndGet();
                lastSpoolEnqueuedAt.set(now);
            }
            case SPOOL_UNAVAILABLE, SPOOL_FAILED -> {
                spoolRejected = spoolRejectedTotal.incrementAndGet();
                lastSpoolRejectedAt.set(now);
            }
            case SYNC_FALLBACK -> {
                syncFallback = syncFallbackTotal.incrementAndGet();
                lastSyncFallbackAt.set(now);
            }
        }
        Level level = switch (outcome) {
            case SPOOLED -> Level.WARNING;
            case SPOOL_UNAVAILABLE, SPOOL_FAILED, SYNC_FALLBACK -> Level.SEVERE;
        };
        logger.log(level, "[FinalSave] Final-save executor rejected " + kind + " save for "
            + uuid + " — " + detail
            + " (outcome=" + outcome
            + ", queueFullTotal=" + queueFullCount
            + ", spooledTotal=" + spooled
            + ", spoolRejectedTotal=" + spoolRejected
            + ", syncFallbackTotal=" + syncFallback + ")", cause);
    }

    // ==================== Spool retryable final save ====================

    /**
     * Attempt to spool a final save directly (used in the QUIT queue-full
     * path where we haven't attempted a DB write yet). Returns the outcome
     * and a detail message suitable for {@link #recordQueueFull}.
     *
     * @param uuid          player UUID
     * @param data          player data to spool
     * @param kind          save kind
     * @param lockSessionId lock session ID (may be null)
     * @return spool result with outcome and detail message
     */
    public SpoolResult trySpoolFinalSave(
            UUID uuid, PlayerData data, SyncManager.SaveKind kind, String lockSessionId) {
        try {
            if (spool != null) {
                EncodedFinalSave encoded = FinalSaveEncoder.encode(
                    uuid, data, kind,
                    config.getClusterId(), config.getServerName(),
                    lockSessionId, config.getCompressionMinSize());
                spool.append(encoded);
                return new SpoolResult(QueueFullOutcome.SPOOLED,
                    "queue full — final save safely spooled to disk for replay");
            } else {
                return new SpoolResult(QueueFullOutcome.SPOOL_UNAVAILABLE,
                    "queue full — spool is not initialized; final state may be lost");
            }
        } catch (Exception spoolError) {
            String msg = spoolError.getMessage() != null
                ? spoolError.getMessage()
                : spoolError.getClass().getSimpleName();
            logger.log(Level.SEVERE, "[FinalSave] CRITICAL: failed to spool final save for "
                + uuid + ". Final state may be lost. Lock will expire naturally.", spoolError);
            return new SpoolResult(QueueFullOutcome.SPOOL_FAILED,
                "queue full — failed to spool final save: " + msg);
        }
    }

    /**
     * Spool a failed final save for later replay.
     *
     * <p>Only retryable failures ({@link SyncManager.SaveResult#isRetryable()})
     * are spooled. Non-retryable failures (FENCING_MISMATCH, VERSION_CONFLICT)
     * are NOT spooled — the lock is lost and replaying the save would clobber
     * newer data.
     *
     * @param uuid          the player UUID
     * @param data          the player data that failed to save
     * @param kind          the save kind (typically QUIT)
     * @param result        the failed save result
     * @param lockSessionId the lock session ID
     * @param detail        human-readable detail for logging
     * @return true if successfully spooled, false otherwise
     */
    public boolean spoolRetryableFinalSave(
            UUID uuid, PlayerData data, SyncManager.SaveKind kind,
            SyncManager.SaveResult result, String lockSessionId, String detail) {
        if (!result.isRetryable()) {
            return false;
        }
        if (spool == null) {
            logger.warning("[FinalSave] Cannot spool retryable save for " + uuid
                + " — spool is not initialized: " + detail);
            return false;
        }
        try {
            EncodedFinalSave encoded = FinalSaveEncoder.encode(
                uuid, data, kind,
                config.getClusterId(), config.getServerName(),
                lockSessionId, config.getCompressionMinSize());
            spool.append(encoded);
            spoolEnqueuedTotal.incrementAndGet();
            lastSpoolEnqueuedAt.set(System.currentTimeMillis());
            logger.info("[FinalSave] Spooled retryable " + kind + " save for " + uuid
                + " (" + result.failureReason() + "): " + detail);
            return true;
        } catch (Exception e) {
            spoolRejectedTotal.incrementAndGet();
            lastSpoolRejectedAt.set(System.currentTimeMillis());
            logger.log(Level.SEVERE, "[FinalSave] CRITICAL: failed to spool retryable save for "
                + uuid + ". Final state may be lost. Lock will expire naturally.", e);
            return false;
        }
    }
}
