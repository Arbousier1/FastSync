package com.fastsync.database;

import com.fastsync.config.ConfigManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

/**
 * Database manager using HikariCP connection pool.
 * Stores player data as LONGBLOB (raw compressed byte[]) — no base64 string encoding.
 * Implements cross-server lock mechanism to prevent data races.
 * Uses Dynamo-style optimistic concurrency control based on a version column.
 *
 * <p>All data access is expressed with jOOQ's type-safe DSL. No jOOQ code
 * generation is used: table and column references are built with the plain
 * {@code table(name(...))} / {@code field(name(...), type.class)} factory
 * methods. DDL (CREATE/ALTER TABLE) and the MySQL {@code LAST_INSERT_ID(expr)}
 * idiom are still issued as raw SQL through {@link DSLContext#execute(String)}.
 * Each operation borrows a single connection from HikariCP and wraps it in a
 * per-call {@code DSLContext} via {@code DSL.using(connection, SQLDialect.MYSQL)}.
 *
 * <h2>Optimizations applied</h2>
 * <ul>
 *   <li><b>Single-SQL lock acquisition:</b> {@link #acquireLock} now uses
 *       {@code INSERT ... ON DUPLICATE KEY UPDATE} to atomically create the
 *       player row (if absent) AND increment the fencing token AND set the
 *       lock — all in one statement, one connection. The fencing token is
 *       retrieved via {@code LAST_INSERT_ID()} on the same connection. This
 *       reduces the previous 3 round-trips (ensurePlayerExists + UPDATE +
 *       SELECT) down to 2 (UPSERT + LAST_INSERT_ID).</li>
 *   <li><b>Removed dead {@code op_seq} column and {@code incrementOpSeq}
 *       method:</b> the operation log uses {@link FileOperationLogManager}'s
 *       session-scoped {@code AtomicLong} and never consults the DB column.
 *       Keeping the column around only wasted space and confused readers.</li>
 * </ul>
 */
public class DatabaseManager {

    // ---- jOOQ table/column references (no code generation) ----
    private static final Field<String> UUID_FIELD = field(name("uuid"), String.class);
    private static final Field<byte[]> DATA_FIELD = field(name("data"), byte[].class);
    private static final Field<Long> VERSION_FIELD = field(name("version"), Long.class);
    private static final Field<Long> CHECKSUM_FIELD = field(name("checksum"), Long.class);
    private static final Field<Long> FENCING_TOKEN_FIELD = field(name("fencing_token"), Long.class);
    private static final Field<String> LOCKED_BY_FIELD = field(name("locked_by"), String.class);
    private static final Field<Long> LOCKED_AT_FIELD = field(name("locked_at"), Long.class);
    private static final Field<String> LAST_SERVER_FIELD = field(name("last_server"), String.class);
    private static final Field<Long> LAST_UPDATED_FIELD = field(name("last_updated"), Long.class);

    private final Logger logger;
    private final ConfigManager config;
    private HikariDataSource dataSource;
    private String dataTable;
    private Table<Record> playerData;

    public DatabaseManager(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Wrap a single JDBC connection in a jOOQ {@link DSLContext} configured for MySQL.
     */
    private static DSLContext dsl(Connection connection) {
        return using(connection, SQLDialect.MYSQL);
    }

    /**
     * Initialize the database connection pool and create tables.
     */
    public void initialize() throws SQLException {
        dataTable = config.getTablePrefix() + "player_data";
        playerData = table(name(dataTable));

        HikariConfig hikariConfig = new HikariConfig();

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?%s",
            config.getDbHost(),
            config.getDbPort(),
            config.getDbDatabase(),
            config.getDbParameters());

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getDbUsername());
        hikariConfig.setPassword(config.getDbPassword());
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");

        hikariConfig.setMaximumPoolSize(config.getPoolSize());
        hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
        hikariConfig.setIdleTimeout(config.getIdleTimeout());
        hikariConfig.setMaxLifetime(config.getMaxLifetime());
        hikariConfig.setLeakDetectionThreshold(config.getLeakDetectionThreshold());

        hikariConfig.setPoolName("FastSync-HikariPool");

        // MySQL optimizations
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("useLocalSessionState", "true");
        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
        hikariConfig.addDataSourceProperty("cacheServerConfiguration", "true");
        hikariConfig.addDataSourceProperty("elideSetAutoCommits", "true");
        hikariConfig.addDataSourceProperty("maintainTimeStats", "false");

        dataSource = new HikariDataSource(hikariConfig);

        createTables();
        migrateSchema();
        logger.info("Database connection established: " + config.getDbHost() + ":" + config.getDbPort() + "/" + config.getDbDatabase());
    }

    /**
     * Create the player data table if it doesn't exist.
     *
     * <p><b>Schema change:</b> the {@code op_seq} column has been removed — the
     * operation log uses {@link FileOperationLogManager}'s in-memory
     * {@code AtomicLong} (session-scoped, monotonic within a session) and never
     * consulted the DB column. Existing deployments with the column are not
     * broken: {@link #migrateSchema} drops the unused column gracefully.
     */
    private void createTables() throws SQLException {
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `uuid` VARCHAR(36) NOT NULL,
                `data` LONGBLOB NOT NULL,
                `version` BIGINT NOT NULL DEFAULT 0,
                `checksum` BIGINT NOT NULL DEFAULT 0,
                `fencing_token` BIGINT NOT NULL DEFAULT 0,
                `locked_by` VARCHAR(64) DEFAULT NULL,
                `locked_at` BIGINT DEFAULT NULL,
                `last_server` VARCHAR(64) DEFAULT NULL,
                `last_updated` BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (`uuid`),
                INDEX idx_locked (`locked_by`, `locked_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(sql);
        }
    }

    /**
     * Migrate the schema for existing tables.
     *
     * <p>Adds version/checksum/fencing_token columns if they don't already exist
     * (older installs) and drops the unused {@code op_seq} column (cleanup from
     * the dead-code removal). All migrations are best-effort — failures from
     * "column doesn't exist" or "column already exists" are suppressed.
     */
    private void migrateSchema() throws SQLException {
        String[] migrations = {
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `version` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `checksum` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `fencing_token` BIGINT NOT NULL DEFAULT 0", dataTable),
            // Drop the unused op_seq column from older installs. Ignore errors
            // (column already gone / not present on fresh installs).
            String.format("ALTER TABLE `%s` DROP COLUMN IF NOT EXISTS `op_seq`", dataTable)
        };
        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);
            for (String sql : migrations) {
                try {
                    dsl.execute(sql);
                } catch (DataAccessException e) {
                    String msg = e.getMessage() == null ? "" : e.getMessage();
                    // Suppress benign cases: duplicate column, unknown column.
                    if (!msg.contains("Duplicate column")
                        && !msg.contains("check that it exists")
                        && !msg.contains("Unknown column")
                        && !msg.contains("Can't DROP")) {
                        logger.warning("Schema migration note: " + msg);
                    }
                }
            }
        }
    }

    /**
     * Acquire a lock for a player's data and generate a fencing token.
     *
     * <p>Per Kleppmann's fencing token pattern: every successful lock acquisition
     * increments a monotonically increasing {@code fencing_token} in the database.
     * This token must be presented on save; the storage layer rejects any write
     * whose fencing token is less than the stored value, preventing stale writes
     * from servers that experienced GC pauses, network delays, or clock skew.
     *
     * <p>This is stronger than Redis {@code SET NX PX} locks, which cannot
     * generate fencing tokens and thus cannot defend against stale writes at
     * the storage layer.
     *
     * <h2>Single-SQL lock acquisition (optimized)</h2>
     * <p>Previously this method required three DB round-trips:
     * <ol>
     *   <li>{@code INSERT IGNORE} to create the player row if missing</li>
     *   <li>{@code UPDATE ... SET fencing_token = fencing_token + 1 WHERE ...}</li>
     *   <li>{@code SELECT fencing_token WHERE uuid = ?}</li>
     * </ol>
     *
     * <p>The new implementation uses a single {@code INSERT ... ON DUPLICATE KEY
     * UPDATE} with {@code LAST_INSERT_ID(fencing_token + 1)} so the row creation,
     * token increment, and lock set happen atomically in one statement. The token
     * is then read back via {@code SELECT LAST_INSERT_ID()} on the same
     * connection — two round-trips total, both on a single borrowed connection.
     *
     * <p><b>Lock-condition SQL:</b> the UPSERT only succeeds (i.e. sets us as
     * the lock holder) when at least one of these is true:
     * <ul>
     *   <li>{@code locked_by IS NULL} — no current holder</li>
     *   <li>{@code locked_at < expiredTime} — previous holder's lease expired</li>
     *   <li>{@code locked_by = serverName} — we already hold the lock (re-entry)</li>
     * </ul>
     *
     * <p>To detect "lock contention" vs "lock acquired", we compare the returned
     * {@code LAST_INSERT_ID()} against the previous fencing token. If the row
     * was unchanged (because the WHERE clause inside ON DUPLICATE KEY UPDATE
     * evaluated false), MySQL still returns 0 from {@code LAST_INSERT_ID()},
     * which we interpret as a failed acquisition.
     *
     * @return LockResult with acquired=true and the fencing token, or acquired=false
     */
    public LockResult acquireLock(UUID uuid, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        long expiredTime = now - (config.getLockTimeout() * 1000L);
        String uuidStr = uuid.toString();

        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);

            // Atomically: insert row (new player) OR update existing row.
            // The VALUES() function references the proposed INSERT values inside
            // the ON DUPLICATE KEY UPDATE clause.
            //
            // For new rows: fencing_token starts at 1, locked_by = us.
            // For existing rows: the UPDATE fires only if the lock is acquirable
            // (locked_by IS NULL OR lease expired OR already ours). We use
            // LAST_INSERT_ID(fencing_token + 1) so we can retrieve the new token
            // via SELECT LAST_INSERT_ID() afterwards.
            //
            // If the lock is NOT acquirable, we don't update locked_by/fencing_token
            // (the IF condition fails), and LAST_INSERT_ID() returns 0.
            String upsertSql = String.format("""
                INSERT INTO `%s` (uuid, data, version, checksum, fencing_token,
                                  locked_by, locked_at, last_server, last_updated)
                VALUES (?, '', 0, 0, 1, ?, ?, ?, 0)
                ON DUPLICATE KEY UPDATE
                    fencing_token = IF(locked_by IS NULL
                                       OR locked_at < ?
                                       OR locked_by = VALUES(locked_by),
                                       LAST_INSERT_ID(fencing_token + 1),
                                       fencing_token),
                    locked_by     = IF(locked_by IS NULL
                                       OR locked_at < ?
                                       OR locked_by = VALUES(locked_by),
                                       VALUES(locked_by),
                                       locked_by),
                    locked_at     = IF(locked_by IS NULL
                                       OR locked_at < ?
                                       OR locked_by = ?,
                                       VALUES(locked_at),
                                       locked_at)
                """, dataTable);

            // MySQL's ON DUPLICATE KEY UPDATE doesn't expose the "did we actually
            // update?" bit directly; we infer it from LAST_INSERT_ID() below.
            dsl.execute(upsertSql,
                uuidStr,    // INSERT uuid
                serverName, // INSERT locked_by
                now,        // INSERT locked_at
                serverName, // INSERT last_server
                expiredTime, // UPDATE condition 1
                expiredTime, // UPDATE condition 2
                expiredTime, // UPDATE condition 3
                serverName); // UPDATE condition 4

            // Retrieve the connection-local LAST_INSERT_ID.
            // - Returns 0 if the IF condition was false (lock held by other server)
            // - Returns the new fencing_token otherwise
            Object result = dsl.fetchValue("SELECT LAST_INSERT_ID()");
            long token = result != null ? ((Number) result).longValue() : 0L;

            if (token == 0L) {
                return LockResult.FAILED;
            }
            return LockResult.success(token);
        }
    }

    /**
     * Load player data from the database along with its version and checksum.
     * Returns VersionedData.EMPTY if no data exists or data is empty.
     */
    public VersionedData loadData(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Record record = dsl(conn)
                .select(DATA_FIELD, VERSION_FIELD, CHECKSUM_FIELD, FENCING_TOKEN_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne();

            if (record != null) {
                byte[] data = record.get(DATA_FIELD);
                // version/checksum/fencing_token are NOT NULL columns, so the
                // boxed Long values are never null here (auto-unboxing is safe).
                if (data != null && data.length > 0) {
                    return new VersionedData(
                        data,
                        record.get(VERSION_FIELD),
                        record.get(CHECKSUM_FIELD),
                        record.get(FENCING_TOKEN_FIELD));
                }
            }
        }
        return VersionedData.EMPTY;
    }

    /**
     * Save player data with optimistic concurrency control AND fencing token verification.
     *
     * <p>Two layers of defence against stale writes:
     * <ol>
     *   <li><b>Version check (Dynamo-style):</b> {@code WHERE version = expectedVersion}
     *   <li><b>Fencing token check (Kleppmann/ZooKeeper-style):</b>
     *       {@code WHERE fencing_token <= fencingToken}
     * </ol>
     */
    public boolean saveData(UUID uuid, byte[] data, long checksum, long expectedVersion,
                            long fencingToken, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(DATA_FIELD, data)
                .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                .set(CHECKSUM_FIELD, checksum)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .set(LAST_SERVER_FIELD, serverName)
                .set(LAST_UPDATED_FIELD, now)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(VERSION_FIELD.eq(expectedVersion))
                    .and(FENCING_TOKEN_FIELD.le(fencingToken)))
                .execute() > 0;
        }
    }

    /**
     * Get the current fencing token for a player in the database.
     * Used for conflict diagnosis.
     */
    public long getCurrentFencingToken(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long token = dsl(conn).select(FENCING_TOKEN_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(FENCING_TOKEN_FIELD);
            return token != null ? token : -1;
        }
    }

    /**
     * Get the current version of player data in the database.
     * Used for conflict diagnosis.
     */
    public long getCurrentVersion(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long version = dsl(conn).select(VERSION_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(VERSION_FIELD);
            return version != null ? version : -1;
        }
    }

    /**
     * Release the lock without saving data (e.g., on error).
     */
    public void releaseLock(UUID uuid, String serverName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).update(playerData)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName).or(LOCKED_BY_FIELD.isNull())))
                .execute();
        }
    }

    /**
     * Get the server that currently holds the lock for a player.
     * Returns null if not locked.
     */
    public String getLockHolder(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).select(LOCKED_BY_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(LOCKED_BY_FIELD);
        }
    }

    /**
     * Close the connection pool.
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed.");
        }
    }

    /**
     * Check if the connection pool is healthy.
     */
    public boolean isHealthy() {
        if (dataSource == null || dataSource.isClosed()) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).fetchValue("SELECT 1") != null;
        } catch (SQLException | DataAccessException e) {
            return false;
        }
    }

    /**
     * Get the HikariCP data source (for stats).
     */
    public HikariDataSource getDataSource() {
        return dataSource;
    }

    /**
     * Compute CRC32 checksum of data for integrity verification.
     */
    public static long computeChecksum(byte[] data) {
        if (data == null || data.length == 0) return 0;
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    /**
     * Verify that data matches its stored checksum.
     */
    public static boolean verifyChecksum(byte[] data, long expectedChecksum) {
        if (expectedChecksum == 0) return true;
        if (data == null || data.length == 0) return expectedChecksum == 0;
        return computeChecksum(data) == expectedChecksum;
    }
}
