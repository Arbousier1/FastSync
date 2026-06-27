package com.fastsync.concurrent;

import org.junit.jupiter.api.Test;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 test: asyncSubmitLogsThrowable.
 *
 * <p>Verifies that {@link AsyncExecutor#submit(Runnable)} catches
 * {@link Throwable} (not just {@link Exception}) and logs it at
 * {@link Level#SEVERE} before rethrowing. This ensures that {@link Error}
 * subtypes ({@link OutOfMemoryError}, {@link StackOverflowError},
 * {@link NoClassDefFoundError}, etc.) are recorded before the worker thread
 * dies — otherwise they would be invisible to operators tailing the log file.
 *
 * <p>Since the test cannot safely throw real Errors (they crash the JVM or
 * test runner), it throws a custom {@link Throwable} subclass and verifies
 * the log handler receives a SEVERE record containing the message.
 */
class AsyncExecutorThrowableTest {

    /**
     * Custom Throwable subclass for testing purposes. Not an Error or
     * Exception, so previous catch(Exception) code would have missed it.
     */
    private static class TestThrowable extends Throwable {
        TestThrowable(String message) {
            super(message);
        }
    }

    @Test
    void asyncSubmitLogsThrowable() throws Exception {
        // Create a test handler that captures SEVERE records.
        Logger logger = Logger.getLogger("async-executor-throwable-test");
        logger.setLevel(Level.ALL);
        TestLogHandler handler = new TestLogHandler();
        logger.addHandler(handler);

        // Create an executor with 1 thread and tiny queue.
        AsyncExecutor executor = new AsyncExecutor(logger, "TestPool", 1, 1);

        // Submit a task that throws TestThrowable.
        TestThrowable thrown = new TestThrowable("test-throwable-message");
        java.util.concurrent.CompletableFuture<Void> future =
            executor.submit(() -> {
                throw thrown;
            });

        // Wait for the future to complete (it should complete exceptionally).
        try {
            future.join();
            fail("Future should have completed exceptionally");
        } catch (java.util.concurrent.CompletionException e) {
            // The cause should be our TestThrowable.
            Throwable cause = e.getCause();
            assertNotNull(cause, "CompletionException must have a cause");
            assertEquals(TestThrowable.class, cause.getClass(),
                "Cause must be TestThrowable, not wrapped in something else");
            assertEquals("test-throwable-message", cause.getMessage());
        }

        // Verify the logger captured a SEVERE record.
        assertTrue(handler.hasSevereRecord,
            "AsyncExecutor.submit must log Throwable at SEVERE before rethrowing");

        // The log record's message should contain our Throwable's message.
        assertNotNull(handler.lastSevereRecord,
            "Handler must have captured a SEVERE LogRecord");
        assertTrue(handler.lastSevereRecord.getMessage().contains("test-throwable-message"),
            "SEVERE log message must contain the Throwable's message");

        // The log record's thrown must be our TestThrowable.
        assertEquals(TestThrowable.class, handler.lastSevereRecord.getThrown().getClass(),
            "LogRecord.getThrown() must be TestThrowable");

        executor.shutdown(1);
    }

    /**
     * Simple LogHandler that captures SEVERE records for assertion.
     */
    private static class TestLogHandler extends Handler {
        volatile boolean hasSevereRecord = false;
        volatile LogRecord lastSevereRecord = null;

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel() == Level.SEVERE) {
                hasSevereRecord = true;
                lastSevereRecord = record;
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() throws SecurityException {}
    }
}