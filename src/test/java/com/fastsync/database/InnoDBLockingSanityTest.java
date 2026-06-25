package com.fastsync.database;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Static audit of SQL strings in the codebase for InnoDB locking best practices.
 *
 * <p>Per MySQL 8.4 InnoDB documentation, the hot path must avoid:
 * <ul>
 *   <li>Range queries combined with {@code FOR UPDATE} (cause next-key locks)
 *   <li>Aggregate queries with {@code FOR UPDATE} on concurrent paths
 *   <li>{@code ORDER BY updated_at/timestamp} for writes or critical reads
 * </ul>
 *
 * <p>This test scans the Java source files and fails if any hot-path SQL
 * violates these rules. It is a regression guard against accidental reintroduction
 * of locking bugs.</p>
 */
class InnoDBLockingSanityTest {

    private static final Path SOURCE_ROOT = Paths.get("src/main/java");

    @Test
    void testNoForUpdateInHotPath() throws IOException {
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            long violations = paths
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(this::containsForUpdate)
                    .count();

            assertEquals(0, violations,
                    "Hot-path SQL must not contain FOR UPDATE (causes next-key locks). " +
                    "Violating files: see test output.");
        }
    }

    @Test
    void testNoTimestampOrderingForWrites() throws IOException {
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            long violations = paths
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(this::containsTimestampOrderForWrite)
                    .count();

            assertEquals(0, violations,
                    "Writes must not use ORDER BY timestamp/updated_at (clock skew unsafe). " +
                    "Use auto-increment id or version instead.");
        }
    }

    private boolean containsForUpdate(Path path) {
        try {
            String content = Files.readString(path);
            return sqlLiteralStream(content).anyMatch(s -> s.toUpperCase().contains("FOR UPDATE"));
        } catch (IOException e) {
            fail("Failed to read " + path, e);
            return false;
        }
    }

    private boolean containsTimestampOrderForWrite(Path path) {
        try {
            String content = Files.readString(path);
            return sqlLiteralStream(content).anyMatch(s -> {
                String upper = s.toUpperCase();
                return (upper.contains("ORDER BY TIMESTAMP") || upper.contains("ORDER BY UPDATED_AT"))
                        && (upper.contains("UPDATE") || upper.contains("DELETE") || upper.contains("SELECT"));
            });
        } catch (IOException e) {
            fail("Failed to read " + path, e);
            return false;
        }
    }

    /**
     * Extract SQL string literals from Java source. This strips out JavaDoc and
     * line comments so that documentation examples do not trigger false positives.
     */
    private Stream<String> sqlLiteralStream(String content) {
        // Remove line comments
        String noLineComments = content.replaceAll("//.*", "");
        // Remove block comments (non-greedy, including JavaDoc)
        String noComments = noLineComments.replaceAll("/\\*[\\s\\S]*?\\*/", "");
        // Extract multi-line SQL strings concatenated with +
        String flattened = noComments.replaceAll("\"\\s*\\+\\s*\"", "");
        // Find remaining double-quoted string literals
        return java.util.regex.Pattern.compile("\"([^\"]*(?:\"\"[^\"]*)*)\"")
                .matcher(flattened)
                .results()
                .map(m -> m.group(1));
    }
}
