package com.fastsync.redis.stream;

/**
 * Type of critical sync event published to Redis Streams.
 *
 * <p>Per the user's architecture guidance, Redis Streams are used for
 * <strong>critical, recoverable events</strong> — not the fire-and-forget
 * Pub/Sub notifications. If a subscriber crashes or restarts, it can
 * recover missed events from the stream's consumer group pending list.
 *
 * <p>Event flow for player handoff:
 * <pre>
 *   Server A (quit):  publish PLAYER_CHECKOUT(uuid, version, fencingToken)
 *   Server B (login): publishes PLAYER_CHECKIN(uuid) after loading data
 * </pre>
 */
public enum StreamEventType {
    /** Player data saved and lock released — player is "checking out" of this server */
    PLAYER_CHECKOUT,
    /** Player data loaded and lock acquired — player has "checked in" to this server */
    PLAYER_CHECKIN,
    /** Server started and is ready to accept players */
    SERVER_START,
    /** Server is shutting down — other servers should be aware */
    SERVER_STOP,
    /** Data conflict detected — published for cross-server awareness */
    DATA_CONFLICT,
    /** Snapshot created — published for cross-server backup awareness */
    SNAPSHOT_CREATED
}
