package com.fastsync.concurrent;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks operation latencies and computes percentile metrics (p50, p99, p99.9).
 *
 * Inspired by Dynamo's focus on p99.9 SLA rather than average/median.
 * The paper explicitly states: "SLAs are expressed and measured at the 99.9
 * percentile of the distribution" because averages mask tail latency.
 *
 * Uses a sliding window of the last N samples to keep memory bounded.
 *
 * <p><b>Performance note:</b> The sample count is tracked with an
 * {@link AtomicInteger} instead of calling {@link ConcurrentLinkedDeque#size()},
 * which is O(n) and would dominate the hot path for large windows. With the
 * counter, {@code record()} is amortized O(1) regardless of window size.
 */
public class LatencyTracker {

    private final String operationName;
    private final Logger logger;
    private final int windowSize;
    private final ConcurrentLinkedDeque<Long> samples = new ConcurrentLinkedDeque<>();
    private final AtomicInteger currentSize = new AtomicInteger(0);
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public LatencyTracker(String operationName, Logger logger, int windowSize) {
        this.operationName = operationName;
        this.logger = logger;
        this.windowSize = windowSize;
    }

    /**
     * Record a latency sample in milliseconds.
     *
     * <p>Amortized O(1): the {@link AtomicInteger} counter avoids the O(n)
     * {@code ConcurrentLinkedDeque.size()} call that previously dominated
     * this method for large windows. In the rare case of concurrent
     * over-eviction (two threads racing), the counter self-corrects on the
     * next {@code record()} call — the window may transiently hold a few
     * fewer samples than {@code windowSize}, which is acceptable for a
     * statistics tracker.
     */
    public void record(long latencyMs) {
        samples.addLast(latencyMs);
        int size = currentSize.incrementAndGet();
        // Evict oldest entries if over the window size.
        // Use a bounded loop to avoid pathological spinning under contention.
        int evictions = 0;
        while (size - evictions > windowSize) {
            if (samples.pollFirst() != null) {
                evictions++;
            } else {
                break; // deque emptied by another thread
            }
        }
        if (evictions > 0) {
            currentSize.addAndGet(-evictions);
        }
        totalOperations.incrementAndGet();
    }

    /**
     * Record an error (operation failed).
     */
    public void recordError() {
        totalErrors.incrementAndGet();
    }

    /**
     * Compute a percentile from the current samples.
     * @param percentile 0-100 (e.g., 99.9 for p99.9)
     * @return the latency at that percentile, or -1 if no samples
     */
    public double getPercentile(double percentile) {
        Long[] arr = samples.toArray(new Long[0]);
        if (arr.length == 0) return -1;

        java.util.Arrays.sort(arr);
        int index = (int) Math.ceil((percentile / 100.0) * arr.length) - 1;
        index = Math.max(0, Math.min(index, arr.length - 1));
        return arr[index];
    }

    /**
     * Log current latency statistics.
     */
    public void logStats() {
        Long[] arr = samples.toArray(new Long[0]);
        if (arr.length == 0) {
            logger.info(String.format("[Latency] %s: no samples yet (total ops: %d, errors: %d)",
                operationName, totalOperations.get(), totalErrors.get()));
            return;
        }

        long sum = 0;
        for (long v : arr) sum += v;
        double avg = (double) sum / arr.length;
        double p50 = getPercentile(50);
        double p99 = getPercentile(99);
        double p999 = getPercentile(99.9);
        long max = arr[0];
        long min = arr[0];
        for (long v : arr) { if (v > max) max = v; if (v < min) min = v; }

        logger.info(String.format(
            "[Latency] %s: samples=%d, avg=%.1fms, p50=%.1fms, p99=%.1fms, p99.9=%.1fms, min=%dms, max=%dms | total_ops=%d, errors=%d",
            operationName, arr.length, avg, p50, p99, p999, min, max,
            totalOperations.get(), totalErrors.get()));
    }

    public long getTotalOperations() { return totalOperations.get(); }
    public long getTotalErrors() { return totalErrors.get(); }
    public int getSampleCount() { return currentSize.get(); }
}
