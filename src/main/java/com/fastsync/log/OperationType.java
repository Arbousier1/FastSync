package com.fastsync.log;

/**
 * Type of operation recorded in the per-UUID operation log.
 *
 * <p>Inspired by Raft's replicated log concept: every state-changing operation
 * for a given UUID is recorded with a monotonically increasing sequence number,
 * creating a deterministic, auditable history. This avoids 90% of cross-server
 * sync bugs because any operation can be traced to its exact position in the
 * per-UUID operation sequence.
 */
public enum OperationType {
    /** Player data loaded from database during login phase */
    LOAD,
    /** Player data saved to database (normal disconnect, death, world save, etc.) */
    SAVE,
    /** Save rejected due to version conflict or fencing token violation */
    CONFLICT,
    /** Snapshot created (backup) */
    SNAPSHOT,
    /** Snapshot restored (rollback) */
    RESTORE,
    /** Lock acquired */
    LOCK_ACQUIRE,
    /** Lock released */
    LOCK_RELEASE,
    /** Lock expired and was force-released */
    LOCK_EXPIRE,
    /** Data checksum verification failed */
    CHECKSUM_FAIL
}
