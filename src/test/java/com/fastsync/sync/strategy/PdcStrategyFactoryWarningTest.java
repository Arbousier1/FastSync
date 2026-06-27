package com.fastsync.sync.strategy;

import com.fastsync.config.ConfigManager;
import org.junit.jupiter.api.Test;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Round 15 test: registeredOnlyEmptyPdcKeysWarns.
 *
 * <p>Verifies that {@link PdcStrategyFactory#create(ConfigManager, Logger)}
 * logs a warning when the mode is "registered-only" and the
 * {@code registered-keys} list is empty. This prevents operators from
 * mistakenly thinking sync-pdc is enabled when no keys will actually be
 * synchronized.
 *
 * <p>The strategy itself is safe (empty keys → no-op), but the warning
 * makes the misconfiguration visible in logs.
 */
class PdcStrategyFactoryWarningTest {

    @Test
    void registeredOnlyEmptyPdcKeysWarns() {
        // Create a test handler that captures WARNING records.
        Logger logger = Logger.getLogger("pdc-strategy-factory-warning-test");
        logger.setLevel(Level.ALL);
        WarningCaptureHandler handler = new WarningCaptureHandler();
        logger.addHandler(handler);

        // Mock ConfigManager to return registered-only mode with empty keys.
        ConfigManager config = mock(ConfigManager.class);
        when(config.getPdcMode()).thenReturn("registered-only");
        when(config.getRegisteredPdcKeys()).thenReturn(java.util.Collections.emptyList());
        when(config.isDebug()).thenReturn(false);

        // Call the factory.
        PdcSyncStrategy strategy = PdcStrategyFactory.create(config, logger);

        // Strategy should be RegisteredKeysPdcStrategy (with empty list).
        assertNotNull(strategy);
        assertEquals(RegisteredKeysPdcStrategy.class, strategy.getClass());

        // Verify the warning was logged.
        assertTrue(handler.hasWarning,
            "PdcStrategyFactory must log a WARNING when registered-only mode has empty keys");

        // The warning message should mention sync-pdc enabled but keys empty.
        assertNotNull(handler.lastWarningRecord);
        String msg = handler.lastWarningRecord.getMessage();
        assertTrue(msg.contains("[PDC]"),
            "Warning must be tagged with [PDC]");
        assertTrue(msg.contains("registered-keys is empty"),
            "Warning must mention registered-keys is empty");
        assertTrue(msg.contains("no PDC keys will be synchronized"),
            "Warning must explain the consequence");
    }

    /**
     * Simple LogHandler that captures WARNING records for assertion.
     */
    private static class WarningCaptureHandler extends Handler {
        volatile boolean hasWarning = false;
        volatile LogRecord lastWarningRecord = null;

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
                hasWarning = true;
                lastWarningRecord = record;
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() throws SecurityException {}
    }
}