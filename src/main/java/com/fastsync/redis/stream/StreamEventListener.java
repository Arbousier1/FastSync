package com.fastsync.redis.stream;

/**
 * Listener for critical sync events from Redis Streams.
 *
 * <p>Implementations are notified when a critical event is received from
 * another server via Redis Streams. Events are guaranteed to be delivered
 * at-least-once (consumer group semantics): if this server crashes while
 * processing, the event will be redelivered on restart via XAUTOCLAIM.
 */
@FunctionalInterface
public interface StreamEventListener {
    void onEvent(StreamEvent event);
}
