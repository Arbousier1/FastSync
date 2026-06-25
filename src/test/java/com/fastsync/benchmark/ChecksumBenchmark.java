package com.fastsync.benchmark;

import com.fastsync.database.DatabaseManager;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for CRC32 checksum computation used to detect data corruption.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class ChecksumBenchmark {

    @Param({"1024", "16384", "65536", "262144"})
    private int dataSize;

    private byte[] data;

    @Setup
    public void setup() {
        data = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(data);
    }

    @Benchmark
    public long computeChecksum() {
        return DatabaseManager.computeChecksum(data);
    }
}
