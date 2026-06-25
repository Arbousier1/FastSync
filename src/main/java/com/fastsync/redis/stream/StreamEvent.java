package com.fastsync.redis.stream;

import java.util.Map;
import java.util.UUID;

/**
 * A critical sync event published to Redis Streams.
 *
 * <p>Redis Streams provide an append-only log with consumer groups, XREADGROUP,
 * XPENDING, and XACK — making events <strong>recoverable</strong>. If a consumer
 * crashes, its unacknowledged events remain in the pending entries list (PEL)
 * and can be reclaimed by another consumer via XAUTOCLAIM.
 *
 * <p>This is fundamentally different from Pub/Sub, which is fire-and-forget:
 * if a subscriber is down when a message is published, the message is lost.
 *
 * <p>Layering strategy (per user's architecture guidance):
 * <ul>
 *   <li><b>Redis Pub/Sub</b> — non-critical notifications (lock release requests)
 *   <li><b>Redis Streams</b> — critical events (player checkout/checkin, server lifecycle)
 *   <li><b>DB Ledger</b> — final source of truth (operation_log, player_data)
 * </ul>
 *
 * @param id          stream entry ID (assigned by Redis XADD, null when publishing)
 * @param type        event type
 * @param uuid        player UUID (null for server-level events)
 * @param server      source server name
 * @param target      target server name (null/empty = broadcast to all)
 * @param version     data version at time of event
 * @param fencingToken fencing token at time of event
 * @param detail      additional detail string
 * @param timestamp   event timestamp (epoch millis)
 */
public record StreamEvent(
    String id,
    StreamEventType type,
    UUID uuid,
    String server,
    String target,
    long version,
    long fencingToken,
    String detail,
    long timestamp
) {
    /**
     * Create a new event for publishing (no ID yet — Redis assigns it).
     */
    public static StreamEvent create(StreamEventType type, UUID uuid, String server,
                                      String target, long version, long fencingToken,
                                      String detail) {
        return new StreamEvent(null, type, uuid, server, target, version,
            fencingToken, detail, System.currentTimeMillis());
    }

    /**
     * Convert to a map of field-value pairs for Redis XADD.
     */
    public Map<String, String> toMap() {
        return Map.of(
            "type", type.name(),
            "uuid", uuid != null ? uuid.toString() : "",
            "server", server != null ? server : "",
            "target", target != null ? target : "",
            "version", String.valueOf(version),
            "fencing_token", String.valueOf(fencingToken),
            "detail", detail != null ? detail.substring(0, Math.min(detail.length(), 500)) : "",
            "timestamp", String.valueOf(timestamp)
        );
    }

    /**
     * Parse a stream entry from Redis XREADGROUP result.
     */
    public static StreamEvent fromMap(String id, Map<String, String> fields) {
        return new StreamEvent(
            id,
            StreamEventType.valueOf(fields.get("type")),
            fields.get("uuid").isEmpty() ? null : UUID.fromString(fields.get("uuid")),
            fields.get("server"),
            fields.get("target"),
            Long.parseLong(fields.get("version")),
            Long.parseLong(fields.get("fencing_token")),
            fields.get("detail"),
            Long.parseLong(fields.get("timestamp"))
        );
    }
}
