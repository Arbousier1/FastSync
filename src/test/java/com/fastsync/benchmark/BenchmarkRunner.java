package com.fastsync.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Entry point for running JMH benchmarks.
 *
 * <p>Run from Maven with:
 * <pre>
 *   mvn test-compile exec:java \
 *     -Dexec.mainClass="com.fastsync.benchmark.BenchmarkRunner" \
 *     -Dexec.classpathScope=test
 * </pre>
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(CompressionBenchmark.class.getSimpleName())
                .include(ChecksumBenchmark.class.getSimpleName())
                .include(StreamEventBenchmark.class.getSimpleName())
                .include(LatencyTrackerBenchmark.class.getSimpleName())
                .forks(0) // run in single JVM because jmh-core is test-scoped
                .warmupIterations(2)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(1))
                .threads(1)
                .build();

        new Runner(options).run();
    }
}
