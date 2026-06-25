package com.fastsync.benchmark;

import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventType;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for Redis Streams event serialization/deserialization.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class StreamEventBenchmark {

    private StreamEvent event;
    private Map<String, String> map;
    private String id;

    @Setup
    public void setup() {
        event = StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT,
                UUID.randomUUID(),
                "survival-1",
                "creative-1",
                42,
                7,
                "cause=disconnect,v=42"
        );
        map = event.toMap();
        id = "1234567890-0";
    }

    @Benchmark
    public Map<String, String> serialize() {
        return event.toMap();
    }

    @Benchmark
    public StreamEvent deserialize() {
        return StreamEvent.fromMap(id, map);
    }
}
