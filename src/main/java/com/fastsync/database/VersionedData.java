package com.fastsync.database;

/**
 * Holds loaded player data with its version, checksum, and fencing token from the database.
 *
 * <p>Used for Dynamo-style optimistic concurrency control combined with
 * Kleppmann-style fencing tokens. All three values are checked on save:
 * <ul>
 *   <li>{@code version} — optimistic concurrency (Dynamo)
 *   <li>{@code checksum} — data integrity (CRC32)
 *   <li>{@code fencingToken} — stale-write defence (ZooKeeper/Chubby leases)
 * </ul>
 *
 * @param data         the compressed data blob (may be null/empty for new players)
 * @param version      the database row version when this data was loaded
 * @param checksum     the CRC32 checksum stored alongside the data
 * @param fencingToken the fencing token assigned when the lock was acquired
 */
public record VersionedData(byte[] data, long version, long checksum, long fencingToken) {

    /**
     * Sentinel for "no data found" (new player, no fencing token yet).
     */
    public static final VersionedData EMPTY = new VersionedData(null, 0, 0, 0);

    public boolean hasData() {
        return data != null && data.length > 0;
    }
}
