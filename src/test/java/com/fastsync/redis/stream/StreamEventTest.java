package com.fastsync.redis.stream;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamEvent} serialization and deserialization.
 *
 * <p>Redis Streams store entries as flat field-value maps. This test ensures
 * that events can be round-tripped without losing critical information such
 * as the fencing token, version, and target server.</p>
 */
class StreamEventTest {

    @Test
    void testRoundTripSerialization() {
        UUID uuid = UUID.randomUUID();
        StreamEvent event = StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT,
                uuid,
                "survival-1",
                "creative-1",
                42,
                7,
                "cause=disconnect"
        );

        Map<String, String> map = event.toMap();
        StreamEvent reconstructed = StreamEvent.fromMap("1234567890-0", map);

        assertEquals("1234567890-0", reconstructed.id());
        assertEquals(StreamEventType.PLAYER_CHECKOUT, reconstructed.type());
        assertEquals(uuid, reconstructed.uuid());
        assertEquals("survival-1", reconstructed.server());
        assertEquals("creative-1", reconstructed.target());
        assertEquals(42, reconstructed.version());
        assertEquals(7, reconstructed.fencingToken());
        assertEquals("cause=disconnect", reconstructed.detail());
        assertTrue(reconstructed.timestamp() > 0);
    }

    @Test
    void testServerLevelEventWithNullUuid() {
        StreamEvent event = StreamEvent.create(
                StreamEventType.SERVER_START,
                null,
                "survival-1",
                "",
                0,
                0,
                "Server started"
        );

        Map<String, String> map = event.toMap();
        StreamEvent reconstructed = StreamEvent.fromMap("999-0", map);

        assertEquals(StreamEventType.SERVER_START, reconstructed.type());
        assertNull(reconstructed.uuid());
        assertEquals("survival-1", reconstructed.server());
        assertEquals("", reconstructed.target());
        assertEquals(0, reconstructed.version());
        assertEquals(0, reconstructed.fencingToken());
    }

    @Test
    void testLongDetailIsTruncated() {
        String longDetail = "x".repeat(600);
        StreamEvent event = StreamEvent.create(
                StreamEventType.DATA_CONFLICT,
                UUID.randomUUID(),
                "survival-1",
                "",
                1,
                2,
                longDetail
        );

        Map<String, String> map = event.toMap();
        assertEquals(500, map.get("detail").length(),
                "Detail should be truncated to 500 characters for Redis field size safety");
    }
}
