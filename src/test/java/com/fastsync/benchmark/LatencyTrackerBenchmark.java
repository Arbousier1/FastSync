package com.fastsync.benchmark;

import com.fastsync.concurrent.LatencyTracker;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * JMH benchmark for percentile latency computation used in Dynamo-style p99.9 SLA tracking.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class LatencyTrackerBenchmark {

    @Param({"100", "1000", "10000"})
    private int windowSize;

    private LatencyTracker tracker;

    @Setup
    public void setup() {
        tracker = new LatencyTracker("load", Logger.getLogger("test"), windowSize);
        for (int i = 0; i < windowSize; i++) {
            tracker.record(ThreadLocalRandom.current().nextInt(100));
        }
    }

    @Benchmark
    public double p999() {
        return tracker.getPercentile(99.9);
    }

    @Benchmark
    public void recordAndWindow() {
        tracker.record(ThreadLocalRandom.current().nextInt(100));
    }
}
