package com.fastsync.sync;

import com.fastsync.concurrent.LatencyTracker;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Dynamo-style p99.9 latency tracking for the three core operations:
 * DB-Load, DB-Save, and Serialize.
 *
 * <p>Extracted from {@link SyncManager} to reduce its class size. Each
 * operation has its own {@link LatencyTracker} with a sliding window.
 * Call sites in SyncManager use {@link #recordLoad(long)}, {@link #recordSave(long)},
 * {@link #recordSerialize(long)} and their error variants — these are
 * null-safe (no-op if {@link #init} has not been called yet).
 */
public class LatencyMonitor {

    private LatencyTracker loadLatency;
    private LatencyTracker saveLatency;
    private LatencyTracker serializeLatency;

    /**
     * Create the three latency trackers.
     *
     * @param logger the plugin logger
     * @param windowSize sliding window size (number of samples)
     */
    public void init(Logger logger, int windowSize) {
        loadLatency = new LatencyTracker("DB-Load", logger, windowSize);
        saveLatency = new LatencyTracker("DB-Save", logger, windowSize);
        serializeLatency = new LatencyTracker("Serialize", logger, windowSize);
    }

    // ==================== Record methods (null-safe) ====================

    public void recordLoad(long latencyMs) {
        if (loadLatency != null) loadLatency.record(latencyMs);
    }

    public void recordLoadError() {
        if (loadLatency != null) loadLatency.recordError();
    }

    public void recordSave(long latencyMs) {
        if (saveLatency != null) saveLatency.record(latencyMs);
    }

    public void recordSaveError() {
        if (saveLatency != null) saveLatency.recordError();
    }

    public void recordSerialize(long latencyMs) {
        if (serializeLatency != null) serializeLatency.record(latencyMs);
    }

    // ==================== Status output ====================

    /**
     * Log latency statistics for all three operations.
     * Called during shutdown and periodic status.
     */
    public void logStats() {
        if (loadLatency != null) loadLatency.logStats();
        if (saveLatency != null) saveLatency.logStats();
        if (serializeLatency != null) serializeLatency.logStats();
    }

    /**
     * Return latency status lines for /fastsync status display.
     *
     * @return list of formatted status lines (p50/p99/p99.9 per operation)
     */
    public List<String> getStatusLines() {
        List<String> lines = new ArrayList<>();
        if (loadLatency != null) lines.add(loadLatency.getStatusLine());
        if (saveLatency != null) lines.add(saveLatency.getStatusLine());
        if (serializeLatency != null) lines.add(serializeLatency.getStatusLine());
        return lines;
    }
}
