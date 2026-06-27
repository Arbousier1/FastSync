package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.conflict.ConflictManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Round 15 production tests for shutdown gate, final-save retry, and
 * component rejection classification.
 *
 * <p>These tests use Mockito + reflection (same pattern as
 * {@link SyncManagerLockReleaseTest}) because:
 * <ul>
 *   <li>{@link SyncManager}'s constructor requires a {@link FastSync} plugin
 *       instance (extends JavaPlugin, needs Paper runtime)</li>
 *   <li>The methods under test are private (beginShutdown,
 *       persistCollectedData)</li>
 *   <li>Testcontainers-based integration tests cover the DB path separately</li>
 * </ul>
 *
 * <p>Setup follows {@link SyncManagerLockReleaseTest}: mock plugin/config/DB,
 * construct SyncManager without calling initialize(), inject mocks for
 * conflictManager/dirtyMask/etc via reflection, invoke private methods.
 */
class SyncManagerRound15Test {

    private FastSync plugin;
    private ConfigManager config;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;
    private Logger testLogger;

    @BeforeEach
    void setup() throws Exception {
        plugin = mock(FastSync.class);
        testLogger = Logger.getLogger("round15-test");
        when(plugin.getLogger()).thenReturn(testLogger);

        config = mock(ConfigManager.class);
        when(config.getServerName()).thenReturn("test-server");
        when(config.isDebug()).thenReturn(false);
        // Component-storage disabled for all tests except component classification
        when(config.isComponentStorageEnabled()).thenReturn(false);
        when(config.getCompressionMinSize()).thenReturn(0);
        when(config.isLatencyTrackingEnabled()).thenReturn(false);

        databaseManager = mock(DatabaseManager.class);

        syncManager = new SyncManager(plugin, config, databaseManager);

        // Inject a mock ConflictManager to avoid NPE in persistCollectedData's
        // conflict-handling branch. The real ConflictManager requires Logger +
        // ConfigManager + SnapshotManager; a mock is simpler and lets us verify
        // calls.
        ConflictManager conflictManager = mock(ConflictManager.class);
        injectField("conflictManager", conflictManager);
    }

    // ==================== Shutdown Gate Tests ====================

    /**
     * Round 15 test: shutdownSetsShuttingDownBeforeSaveAll.
     *
     * <p>Verifies that calling {@code savePlayersSnapshot(SaveKind.SHUTDOWN)}
     * sets the {@code shuttingDown} flag before dispatching saves (belt-and-
     * suspenders guard). The flag is set at the start of the method, so any
     * periodic task that fires during the shutdown window is rejected.
     */
    @Test
    void shutdownSetsShuttingDownBeforeSaveAll() throws Exception {
        // Reset shuttingDown to false (default)
        setShuttingDown(false);

        // Simulate an empty player list — we only care about the flag.
        var result = syncManager.savePlayersSnapshot(
            new java.util.ArrayList<>(),
            SyncManager.SaveKind.SHUTDOWN);

        // The flag must now be true.
        boolean flag = getShuttingDown();
        assertTrue(flag, "savePlayersSnapshot(SHUTDOWN) must set shuttingDown=true before dispatch");

        // The result should be 0/0/0 for empty list.
        assertEquals(0, result.total());
        assertEquals(0, result.success());
        assertEquals(0, result.failed());
    }

    /**
     * Round 15 test: savePlayerAsyncRejectedAfterBeginShutdown.
     *
     * <p>Verifies that {@code savePlayerAsync} rejects new saves after
     * {@code beginShutdown()} is called. This prevents a periodic/death/
     * world_save task from racing with the SHUTDOWN save and overwriting the
     * final state.
     */
    @Test
    void savePlayerAsyncRejectedAfterBeginShutdown() throws Exception {
        // Add a UUID to activePlayers so the method proceeds past the first check.
        ConcurrentHashMap<UUID, Boolean> activePlayers = getField("activePlayers");
        UUID testUuid = UUID.randomUUID();
        activePlayers.put(testUuid, true);

        // Call beginShutdown (public method).
        syncManager.beginShutdown();
        assertTrue(getShuttingDown(), "beginShutdown must set shuttingDown=true");

        // Create a mock Player for the call.
        org.bukkit.entity.Player player = mock(org.bukkit.entity.Player.class);
        when(player.getUniqueId()).thenReturn(testUuid);

        // Call savePlayerAsync with a valid online-save kind.
        // It should return early (no DB interaction) due to the flag.
        syncManager.savePlayerAsync(player, SyncManager.SaveKind.PERIODIC);

        // Verify that databaseManager was never touched.
        verifyNoInteractions(databaseManager);
    }

    // ==================== Final Save Retry Tests ====================

    /**
     * Round 15 test: finalSaveRetriesSameSessionVersionConflictThreeTimes.
     *
     * <p>Verifies that a final save (QUIT/SHUTDOWN) retries up to 3 times when
     * the CAS fails due to a same-fencing self-conflict (our own previous
     * online save advanced the version while we waited for the saveLock).
     *
     * <p>Setup:
     * <ul>
     *   <li>Stub saveDataAndReleaseLockClearComponents to return false on
     *       first 2 attempts, true on 3rd</li>
     *   <li>Stub getLockState to return a LockState where locked_by/serverName,
     *       fencingToken, lockSessionId all match and version > expected</li>
     *   <li>Invoke persistCollectedData with SaveKind.QUIT (releaseLock=true)</li>
     *   <li>Verify 3 save calls + success</li>
     * </ul>
     */
    @Test
    void finalSaveRetriesSameSessionVersionConflictThreeTimes() throws Exception {
        UUID uuid = UUID.randomUUID();
        PlayerData data = new PlayerData();
        data.setVersion(5);
        data.setFencingToken(10);

        // Set up playerLockSessions with a session ID for this UUID.
        ConcurrentHashMap<UUID, String> sessions = getField("playerLockSessions");
        String sessionId = "session-quit-test";
        sessions.put(uuid, sessionId);

        // Stub databaseManager:
        // - First 2 save attempts fail (CAS conflict), 3rd succeeds.
        // - getLockState returns a LockState indicating lock is still ours
        //   and version advanced from 5 → 6 → 7.
        when(databaseManager.saveDataAndReleaseLockClearComponents(
                any(UUID.class), any(byte[].class), anyLong(), anyLong(), anyLong(), any(String.class), any(String.class)))
            .thenReturn(false)   // attempt 1
            .thenReturn(false)   // attempt 2
            .thenReturn(true);   // attempt 3

        // LockState that matches server/fencing/session and has version > expected.
        DatabaseManager.LockState lockState = mock(DatabaseManager.LockState.class);
        when(lockState.version()).thenReturn(7L);  // > expected 5, then 6
        when(lockState.fencingToken()).thenReturn(10L);
        when(lockState.lockedBy()).thenReturn("test-server");
        when(lockState.lockSessionId()).thenReturn(sessionId);
        when(lockState.rowExists()).thenReturn(true);
        when(lockState.isHeldBy("test-server", 10L, sessionId)).thenReturn(true);

        when(databaseManager.getLockState(any(UUID.class))).thenReturn(lockState);

        // Invoke persistCollectedData via reflection (private method).
        SyncManager.SaveResult result = invokePersistCollectedData(
            uuid, data, SyncManager.SaveKind.QUIT,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY);

        // Verify success after 3 attempts.
        assertTrue(result.success(), "Final save must succeed after 3 retries on same-fencing self-conflict");

        // Verify exactly 3 save calls.
        verify(databaseManager, times(3))
            .saveDataAndReleaseLockClearComponents(
                eq(uuid), any(byte[].class), anyLong(), anyLong(), eq(10L),
                eq("test-server"), eq(sessionId));

        // Verify getLockState was called after each failed attempt (2 times).
        verify(databaseManager, times(2)).getLockState(eq(uuid));
    }

    /**
     * Round 15 test: finalSaveDoesNotRetryOnSessionMismatch.
     *
     * <p>Verifies that a final save does NOT retry when the lock was stolen
     * (session mismatch). The CAS fails, getLockState reveals a different
     * session, and the retry loop breaks immediately.
     *
     * <p>Setup:
     * <ul>
     *   <li>Stub saveDataAndReleaseLockClearComponents to return false</li>
     *   <li>Stub getLockState to return a LockState where session differs</li>
     *   <li>Invoke persistCollectedData with SaveKind.QUIT</li>
     *   <li>Verify only 1 save attempt, no retry, result is error with
     *       FENCING_MISMATCH</li>
     * </ul>
     */
    @Test
    void finalSaveDoesNotRetryOnSessionMismatch() throws Exception {
        UUID uuid = UUID.randomUUID();
        PlayerData data = new PlayerData();
        data.setVersion(5);
        data.setFencingToken(10);

        ConcurrentHashMap<UUID, String> sessions = getField("playerLockSessions");
        String ourSession = "session-ours";
        sessions.put(uuid, ourSession);

        // Stub save to fail (CAS conflict).
        when(databaseManager.saveDataAndReleaseLockClearComponents(
                any(UUID.class), any(byte[].class), anyLong(), anyLong(), anyLong(), any(String.class), any(String.class)))
            .thenReturn(false);

        // LockState shows lock held by different session.
        DatabaseManager.LockState lockState = mock(DatabaseManager.LockState.class);
        when(lockState.version()).thenReturn(6L);
        when(lockState.fencingToken()).thenReturn(10L);
        when(lockState.lockedBy()).thenReturn("test-server");
        when(lockState.lockSessionId()).thenReturn("session-other");  // mismatch!
        when(lockState.rowExists()).thenReturn(true);
        when(lockState.isHeldBy("test-server", 10L, ourSession)).thenReturn(false);

        when(databaseManager.getLockState(any(UUID.class))).thenReturn(lockState);

        SyncManager.SaveResult result = invokePersistCollectedData(
            uuid, data, SyncManager.SaveKind.QUIT,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY);

        // Must NOT succeed.
        assertFalse(result.success(), "Final save must fail when session mismatch detected");

        // Must be FENCING_MISMATCH (lock lost).
        assertEquals(SyncManager.SaveFailureReason.FENCING_MISMATCH,
            result.failureReason(), "Failure reason must be FENCING_MISMATCH on session mismatch");

        // Only 1 save attempt (no retry).
        verify(databaseManager, times(1))
            .saveDataAndReleaseLockClearComponents(
                eq(uuid), any(byte[].class), anyLong(), anyLong(), eq(10L),
                eq("test-server"), eq(ourSession));

        // getLockState called once after failure.
        verify(databaseManager, times(1)).getLockState(eq(uuid));
    }

    // ==================== Component Rejection Classification Tests ====================

    /**
     * Round 15 test: componentStaleVersionDoesNotFallbackFullBlob.
     *
     * <p>Verifies that when the component path sees a stale collected version
     * ({@code ComponentRejectReason.STALE_VERSION}), the result is
     * {@code ComponentSaveOutcome.decision == SKIP_STALE_ONLINE_SAVE} and
     * persistCollectedData returns an error without falling back to full Blob.
     *
     * <p>This is the core P0 fix: stale version must NOT clobber newer state.
     */
    @Test
    void componentStaleVersionDoesNotFallbackFullBlob() throws Exception {
        UUID uuid = UUID.randomUUID();
        PlayerData data = new PlayerData();
        data.setVersion(3);
        data.setFencingToken(5);

        // Enable component-storage for this test.
        when(config.isComponentStorageEnabled()).thenReturn(true);

        // Inject a non-null dirtyMask so the component path is taken.
        com.fastsync.sync.dirty.ComponentDirtyMask dirtyMask =
            mock(com.fastsync.sync.dirty.ComponentDirtyMask.class);
        when(dirtyMask.isAnyDirty(uuid)).thenReturn(true);
        // Provide a non-empty dirty snapshot.
        java.util.Set<com.fastsync.sync.dirty.ComponentDirtyMask.Component> dirtySet =
            java.util.Set.of(com.fastsync.sync.dirty.ComponentDirtyMask.Component.INVENTORY);
        com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot snapshot =
            mock(com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.class);
        when(snapshot.isEmpty()).thenReturn(false);
        when(snapshot.components()).thenReturn(dirtySet);
        injectField("dirtyMask", dirtyMask);

        // Put UUID in playersWithBaseline so the baseline gate passes.
        java.util.Set<UUID> baseline = getField("playersWithBaseline");
        baseline.add(uuid);

        // Stub config for component sync enabled.
        when(config.isSyncInventory()).thenReturn(true);
        when(config.getComponentBatchSize()).thenReturn(10);

        // Stub databaseManager.upsertComponentsIfLockHeld to return
        // STALE_VERSION rejection.
        DatabaseManager.ComponentBatchResult rejected =
            DatabaseManager.ComponentBatchResult.rejected(
                "stale collected version", DatabaseManager.ComponentRejectReason.STALE_VERSION);
        when(databaseManager.upsertComponentsIfLockHeld(
                any(UUID.class), anyMap(), anyMap(), any(String.class), anyLong(),
                any(String.class), anyLong(), anyLong()))
            .thenReturn(rejected);

        // Stub playerLockSessions.
        ConcurrentHashMap<UUID, String> sessions = getField("playerLockSessions");
        sessions.put(uuid, "session-stale");

        // Invoke persistCollectedData (online save, releaseLock=false).
        SyncManager.SaveResult result = invokePersistCollectedData(
            uuid, data, SyncManager.SaveKind.PERIODIC, snapshot);

        // Must NOT succeed (skip).
        assertFalse(result.success(), "Stale version component save must return error (skip)");

        // Must be VERSION_CONFLICT (the mapping from STALE_VERSION in persistCollectedData).
        assertEquals(SyncManager.SaveFailureReason.VERSION_CONFLICT,
            result.failureReason(), "Stale version must map to VERSION_CONFLICT, not fall back");

        // Full Blob save must NOT be called.
        verify(databaseManager, never())
            .saveDataKeepLockClearComponents(
                any(UUID.class), any(byte[].class), anyLong(), anyLong(), anyLong(),
                any(String.class), any(String.class));
    }

    /**
     * Round 15 test: componentLockMismatchDoesNotFallbackFullBlob.
     *
     * <p>Verifies that when the component path sees a lock/fencing/session
     * mismatch ({@code ComponentRejectReason.LOCK_OR_FENCING_MISMATCH} or
     * {@code SESSION_MISMATCH}), the result is
     * {@code ComponentSaveOutcome.decision == FATAL_LOCK_CONFLICT} and
     * persistCollectedData returns an error without falling back to full Blob.
     *
     * <p>This is the other core P0 fix: lock loss must NOT overwrite newer data.
     */
    @Test
    void componentLockMismatchDoesNotFallbackFullBlob() throws Exception {
        UUID uuid = UUID.randomUUID();
        PlayerData data = new PlayerData();
        data.setVersion(3);
        data.setFencingToken(5);

        when(config.isComponentStorageEnabled()).thenReturn(true);

        com.fastsync.sync.dirty.ComponentDirtyMask dirtyMask =
            mock(com.fastsync.sync.dirty.ComponentDirtyMask.class);
        when(dirtyMask.isAnyDirty(uuid)).thenReturn(true);
        java.util.Set<com.fastsync.sync.dirty.ComponentDirtyMask.Component> dirtySet =
            java.util.Set.of(com.fastsync.sync.dirty.ComponentDirtyMask.Component.VITALS);
        com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot snapshot =
            mock(com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.class);
        when(snapshot.isEmpty()).thenReturn(false);
        when(snapshot.components()).thenReturn(dirtySet);
        injectField("dirtyMask", dirtyMask);

        java.util.Set<UUID> baseline = getField("playersWithBaseline");
        baseline.add(uuid);

        when(config.isSyncHealth()).thenReturn(true);
        when(config.getComponentBatchSize()).thenReturn(10);

        // Stub upsertComponentsIfLockHeld to return SESSION_MISMATCH.
        DatabaseManager.ComponentBatchResult rejected =
            DatabaseManager.ComponentBatchResult.rejected(
                "lock_session_id mismatch", DatabaseManager.ComponentRejectReason.SESSION_MISMATCH);
        when(databaseManager.upsertComponentsIfLockHeld(
                any(UUID.class), anyMap(), anyMap(), any(String.class), anyLong(),
                any(String.class), anyLong(), anyLong()))
            .thenReturn(rejected);

        ConcurrentHashMap<UUID, String> sessions = getField("playerLockSessions");
        sessions.put(uuid, "session-mismatch-test");

        SyncManager.SaveResult result = invokePersistCollectedData(
            uuid, data, SyncManager.SaveKind.PERIODIC, snapshot);

        assertFalse(result.success(), "Lock mismatch must return error, not fall back");

        // Must be FENCING_MISMATCH (the mapping in persistCollectedData for
        // LOCK_OR_FENCING_MISMATCH / SESSION_MISMATCH).
        assertEquals(SyncManager.SaveFailureReason.FENCING_MISMATCH,
            result.failureReason(), "Lock/session mismatch must map to FENCING_MISMATCH");

        // Full Blob save must NOT be called.
        verify(databaseManager, never())
            .saveDataKeepLockClearComponents(
                any(UUID.class), any(byte[].class), anyLong(), anyLong(), anyLong(),
                any(String.class), any(String.class));
    }

    // ==================== Reflection Helpers ====================

    private void setShuttingDown(boolean value) throws Exception {
        Field f = SyncManager.class.getDeclaredField("shuttingDown");
        f.setAccessible(true);
        f.set(syncManager, value);
    }

    private boolean getShuttingDown() throws Exception {
        Field f = SyncManager.class.getDeclaredField("shuttingDown");
        f.setAccessible(true);
        return f.getBoolean(syncManager);
    }

    private <T> T getField(String name) throws Exception {
        Field f = SyncManager.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(syncManager);
    }

    private void injectField(String name, Object value) throws Exception {
        Field f = SyncManager.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(syncManager, value);
    }

    private SyncManager.SaveResult invokePersistCollectedData(
            UUID uuid, PlayerData data, SyncManager.SaveKind kind,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot snapshot)
            throws Exception {
        Method m = SyncManager.class.getDeclaredMethod(
            "persistCollectedData", UUID.class, PlayerData.class,
            SyncManager.SaveKind.class,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.class);
        m.setAccessible(true);
        return (SyncManager.SaveResult) m.invoke(syncManager, uuid, data, kind, snapshot);
    }
}