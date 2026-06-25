package com.fastsync.log;

import java.util.UUID;

/**
 * An entry in the per-UUID append-only operation log.
 *
 * <p>This implements the Raft-inspired concept of an ordered log per entity.
 * Each entry records WHO did WHAT WHEN, with a per-UUID sequence number that
 * monotonically increases. This creates a deterministic, auditable history
 * that enables:
 * <ul>
 *   <li>Conflict diagnosis: exact sequence of events leading to a stale write
 *   <li>Recovery: replay log to reconstruct state machine transitions
 *   <li>Audit: full trace of data ownership transfers across servers
 * </ul>
 *
 * <p>Key fields:
 * <ul>
 *   <li>{@code seq} — per-UUID sequence number (like Raft log index). Assigned
 *       atomically by the database. Two operations on the same UUID can never
 *       have the same seq.
 *   <li>{@code fencingToken} — the fencing token active at the time of the
 *       operation. Correlates with the fencing token defence layer.
 *   <li>{@code version} — the data version before/after the operation.
 *   <li>{@code dataSize} — size of the data blob involved (for audit).
 * </ul>
 *
 * @param seq           per-UUID monotonic sequence number
 * @param uuid          player UUID
 * @param type          operation type
 * @param serverName    server that performed the operation
 * @param fencingToken  fencing token at time of operation
 * @param version       data version involved
 * @param dataSize      size of data blob in bytes (0 if N/A)
 * @param detail        human-readable detail (e.g., "expected v5, actual v6")
 * @param timestamp     operation timestamp (epoch millis)
 */
public record OperationLog(
    long seq,
    UUID uuid,
    OperationType type,
    String serverName,
    long fencingToken,
    long version,
    int dataSize,
    String detail,
    long timestamp
) {
    /**
     * Create a log entry for insertion (seq will be assigned by DB).
     */
    public static OperationLog create(UUID uuid, OperationType type, String serverName,
                                       long fencingToken, long version, int dataSize,
                                       String detail) {
        return new OperationLog(0, uuid, type, serverName, fencingToken, version,
            dataSize, detail, System.currentTimeMillis());
    }
}
