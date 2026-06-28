package com.fastsync.spool;

import com.fastsync.database.DatabaseManager;
import org.bukkit.plugin.Plugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FinalSaveSpoolTest {

    private static final Logger LOGGER = Logger.getLogger(FinalSaveSpoolTest.class.getName());

    @TempDir
    Path tempDir;

    @Test
    void listsNewestFinalStateFirstEvenWhenAppendedOutOfOrder() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 7);
        UUID uuid = UUID.randomUUID();
        long now = System.currentTimeMillis();

        spool.append(encoded(uuid, 2, now));
        spool.append(encoded(uuid, 1, now - 1_000));

        List<Path> pending = spool.listPending(10);
        assertEquals(2, pending.size());
        assertEquals(2, spool.read(pending.get(0)).expectedVersion());
        assertEquals(1, spool.read(pending.get(1)).expectedVersion());
    }

    @Test
    void rewriteIsInPlaceAndPersistsAttemptCount() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 7);
        UUID uuid = UUID.randomUUID();
        long createdAt = System.currentTimeMillis();
        spool.append(encoded(uuid, 4, createdAt));
        Path file = spool.listPending(1).getFirst();
        long initialBytes = spool.getTotalBytes();

        spool.rewriteWithUpdatedVersion(file, 5, "version advanced");

        assertTrue(Files.exists(file));
        assertEquals(1, spool.getPendingCount());
        assertTrue(spool.getTotalBytes() >= initialBytes);
        FinalSaveSpoolRecord record = spool.read(file);
        assertEquals(FinalSaveSpoolRecord.CURRENT_FORMAT, record.formatVersion());
        assertEquals(5, record.expectedVersion());
        assertEquals(1, record.attempts());
        assertEquals(createdAt, record.createdAt());
        assertEquals("version advanced", record.lastError());

        FinalSaveSpool reopened = spool(tempDir, 10, 1_000_000, 7);
        assertEquals(1, reopened.read(reopened.listPending(1).getFirst()).attempts());
    }

    @Test
    void enforcesLimitsAndReleasesCapacityAfterMove() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 1, 1_000_000, 7);
        spool.append(encoded(UUID.randomUUID(), 1, System.currentTimeMillis()));

        assertThrows(java.io.IOException.class,
            () -> spool.append(encoded(UUID.randomUUID(), 1, System.currentTimeMillis())));

        spool.moveToDone(spool.listPending(1).getFirst());
        assertEquals(0, spool.getPendingCount());
        assertEquals(0, spool.getTotalBytes());

        spool.append(encoded(UUID.randomUUID(), 1, System.currentTimeMillis()));
        assertEquals(1, spool.getPendingCount());

        FinalSaveSpool byteLimited = spool(tempDir.resolve("tiny"), 10, 1, 7);
        assertThrows(java.io.IOException.class,
            () -> byteLimited.append(encoded(UUID.randomUUID(), 1, System.currentTimeMillis())));
        assertEquals(0, byteLimited.getPendingCount());
        assertTrue(byteLimited.listPending(10).isEmpty());
    }

    @Test
    void expiresFailedRecordsAndSidecars() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 1);
        spool.append(encoded(UUID.randomUUID(), 1, System.currentTimeMillis()));
        spool.moveToFailed(spool.listPending(1).getFirst(), "permanent failure");
        assertEquals(1, spool.getFailedCount());

        Path failedDir = tempDir.resolve("failed");
        try (var files = Files.list(failedDir)) {
            for (Path file : files.toList()) {
                Files.setLastModifiedTime(file,
                    FileTime.from(Instant.now().minus(2, ChronoUnit.DAYS)));
            }
        }

        spool.cleanupExpiredFailed();
        assertEquals(0, spool.getFailedCount());
        try (var files = Files.list(failedDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void readsLegacyV1RecordAndRejectsUnboundedBlobLength() throws Exception {
        Path pending = tempDir.resolve("pending");
        Files.createDirectories(pending);
        UUID uuid = UUID.randomUUID();
        Path legacy = pending.resolve("legacy.fspool");
        writeV1(legacy, uuid, 3);

        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 7);
        FinalSaveSpoolRecord record = spool.read(legacy);
        assertEquals(1, record.formatVersion());
        assertEquals(0, record.attempts());
        assertEquals(3, record.expectedVersion());

        Path corrupt = pending.resolve("corrupt.fspool");
        writeHeaderWithBlobLength(corrupt, Integer.MAX_VALUE);
        assertThrows(java.io.IOException.class, () -> spool.read(corrupt));
    }

    @Test
    void replayRejectsAnotherClusterWithoutCallingSave() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 7);
        EncodedFinalSave foreign = new EncodedFinalSave(
            UUID.randomUUID(), "foreign", "server-a", "session-a",
            1, 7, 9, new byte[]{1, 2}, "QUIT", System.currentTimeMillis());
        spool.append(foreign);

        DatabaseManager database = mock(DatabaseManager.class);
        when(database.getClusterId()).thenReturn("local");
        FinalSaveReplayService replay = replay(spool, database);

        invokeReplayBatch(replay);

        assertEquals(0, spool.getPendingCount());
        assertEquals(1, spool.getFailedCount());
        verify(database, never()).saveDataAndReleaseLockClearComponents(
            any(), any(), anyLong(), anyLong(), anyLong(), any(), any());
    }

    @Test
    void replayPersistsRetriesAndStopsAfterConfiguredMaximum() throws Exception {
        FinalSaveSpool spool = spool(tempDir, 10, 1_000_000, 7);
        UUID uuid = UUID.randomUUID();
        spool.append(encoded(uuid, 1, System.currentTimeMillis()));

        DatabaseManager database = mock(DatabaseManager.class);
        when(database.getClusterId()).thenReturn("cluster-a");
        when(database.saveDataAndReleaseLockClearComponents(
            eq(uuid), any(), anyLong(), anyLong(), eq(7L), eq("server-a"), eq("session-a")))
            .thenReturn(false);
        when(database.getLockState(uuid))
            .thenReturn(new DatabaseManager.LockState(2, 7, "server-a", "session-a", true))
            .thenReturn(new DatabaseManager.LockState(3, 7, "server-a", "session-a", true))
            .thenReturn(new DatabaseManager.LockState(4, 7, "server-a", "session-a", true));
        FinalSaveReplayService replay = replay(spool, database);

        invokeReplayBatch(replay);
        FinalSaveSpoolRecord firstRetry = spool.read(spool.listPending(1).getFirst());
        assertEquals(1, firstRetry.attempts());
        assertEquals(2, firstRetry.expectedVersion());

        invokeReplayBatch(replay);
        FinalSaveSpoolRecord secondRetry = spool.read(spool.listPending(1).getFirst());
        assertEquals(2, secondRetry.attempts());
        assertEquals(3, secondRetry.expectedVersion());

        invokeReplayBatch(replay);
        assertEquals(0, spool.getPendingCount());
        assertEquals(1, spool.getFailedCount());
        verify(database, times(3)).saveDataAndReleaseLockClearComponents(
            eq(uuid), any(), anyLong(), anyLong(), eq(7L), eq("server-a"), eq("session-a"));
    }

    private FinalSaveSpool spool(Path root, long maxFiles, long maxBytes, int retainDays)
            throws Exception {
        return new FinalSaveSpool(LOGGER, root, false, maxFiles, maxBytes, retainDays);
    }

    private EncodedFinalSave encoded(UUID uuid, long version, long createdAt) {
        return new EncodedFinalSave(
            uuid, "cluster-a", "server-a", "session-a", version, 7, 9,
            new byte[]{1, 2}, "QUIT", createdAt);
    }

    private FinalSaveReplayService replay(FinalSaveSpool spool, DatabaseManager database) {
        return new FinalSaveReplayService(
            LOGGER, spool, database, mock(Plugin.class), 10, 100, "server-a", null);
    }

    private void invokeReplayBatch(FinalSaveReplayService replay) throws Exception {
        var method = FinalSaveReplayService.class.getDeclaredMethod("replayBatchExclusive");
        method.setAccessible(true);
        method.invoke(replay);
    }

    private void writeV1(Path file, UUID uuid, long expectedVersion) throws Exception {
        try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            writeCommonHeader(out, 1, uuid, expectedVersion);
            out.writeInt(2);
            out.write(new byte[]{1, 2});
        }
    }

    private void writeHeaderWithBlobLength(Path file, int blobLength) throws Exception {
        try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            writeCommonHeader(out, 1, UUID.randomUUID(), 1);
            out.writeInt(blobLength);
        }
    }

    private void writeCommonHeader(DataOutputStream out, int format, UUID uuid,
                                   long expectedVersion) throws Exception {
        out.writeUTF("FASTSYNC_FINAL_SPOOL");
        out.writeInt(format);
        out.writeUTF(uuid.toString());
        out.writeUTF("cluster-a");
        out.writeUTF("server-a");
        out.writeUTF("session-a");
        out.writeLong(expectedVersion);
        out.writeLong(7);
        out.writeLong(9);
        out.writeUTF("QUIT");
        out.writeLong(System.currentTimeMillis());
    }
}
