package com.fastsync.redis;

import com.fastsync.config.ConfigManager;
import com.fastsync.redis.message.LockReleasedMessage;
import com.fastsync.redis.message.LockRequestMessage;
import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.redis.messagebroker.Logger;
import net.momirealms.sparrow.redis.messagebroker.MessageBroker;
import net.momirealms.sparrow.redis.messagebroker.connection.PubSubRedisConnection;
import net.momirealms.sparrow.redis.messagebroker.connection.RedisConnection;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Redis manager for cross-server lock coordination.
 *
 * <p>This used to be a hand-written Jedis pub/sub implementation. It now uses
 * the sparrow-redis-message-broker library, which provides typed one-way
 * messages with built-in server routing and automatic reconnection.
 *
 * Protocol (unchanged behaviour, new transport):
 *   1. Server A publishes a {@link LockRequestMessage} (broadcast) when it
 *      cannot acquire the lock for a player.
 *   2. Server B (holding the lock) receives the request. It cannot release
 *      immediately because it is still saving, so it just logs the request.
 *   3. When Server B finishes saving, it publishes a
 *      {@link LockReleasedMessage} (broadcast).
 *   4. Server A receives the released message and counts down the latch it is
 *      waiting on, retrying immediately (no sleep/polling).
 *
 * If no released message is received within the timeout, Server A falls back to
 * the database-level lock check (which handles stale locks).
 *
 * The public API is intentionally identical to the previous Jedis version so
 * that {@code SyncManager} and {@code FastSync} require no changes.
 */
public class RedisManager {

    /** Lettuce request-queue size used for the pub/sub connection. */
    private static final int QUEUE_SIZE = 1000;

    /** Single broker channel all FastSync messages are exchanged on. */
    private static final byte[] CHANNEL = "fastsync:messages".getBytes(StandardCharsets.UTF_8);

    /**
     * Singleton reference used by the (static) message callbacks to reach back
     * into the manager instance that registered them.
     */
    private static volatile RedisManager instance;

    private final java.util.logging.Logger logger;
    private final ConfigManager config;
    private final String serverName;

    // Track lock release notifications: UUID -> CountDownLatch.
    // Created when we start waiting for a lock and counted down when the
    // RELEASED message arrives.
    private final ConcurrentHashMap<UUID, CountDownLatch> releaseWaiters = new ConcurrentHashMap<>();

    private MessageBroker<ByteBuf> broker;

    public RedisManager(java.util.logging.Logger logger, ConfigManager config, String serverName) {
        this.logger = logger;
        this.config = config;
        this.serverName = serverName;
        instance = this;
    }

    /**
     * Build the Redis URI, create the pub/sub connection and the message broker,
     * register the lock messages and start listening.
     *
     * @throws RuntimeException if Redis cannot be reached
     */
    public void initialize() {
        String password = config.getRedisPassword();
        String scheme = config.isRedisSsl() ? "rediss://" : "redis://";
        String auth = (password != null && !password.isEmpty()) ? ":" + password + "@" : "";
        String redisUri = scheme + auth + config.getRedisHost() + ":" + config.getRedisPort()
            + "/" + config.getRedisDatabase();

        // The connection is established synchronously inside the constructor;
        // if Redis is unreachable this throws immediately.
        RedisConnection connection;
        try {
            connection = new PubSubRedisConnection(redisUri, QUEUE_SIZE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Redis at " +
                config.getRedisHost() + ":" + config.getRedisPort(), e);
        }

        // Bridge the broker's Logger onto the plugin's java.util.logging.Logger.
        Logger brokerLogger = new Logger() {
            @Override
            public void error(String msg, Throwable t) { logger.log(Level.SEVERE, msg, t); }

            @Override
            public void warn(String msg, Throwable t) { logger.log(Level.WARNING, msg, t); }

            @Override
            public void info(String msg) { logger.info(msg); }

            @Override
            public void debug(String msg) { if (config.isDebug()) logger.info(msg); }
        };

        try {
            broker = MessageBroker.<ByteBuf>builder(buf -> buf)
                .logger(brokerLogger)
                .connection(connection)
                .channel(CHANNEL)
                .serverId(serverName)
                .tags(Set.of())
                .build();

            broker.registry().register(LockRequestMessage.IDENTIFIER, LockRequestMessage.CODEC);
            broker.registry().register(LockReleasedMessage.IDENTIFIER, LockReleasedMessage.CODEC);

            broker.subscribe();
        } catch (Exception e) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // best effort
            }
            throw new RuntimeException("Failed to initialize Redis message broker at " +
                config.getRedisHost() + ":" + config.getRedisPort(), e);
        }

        logger.info("Redis connected: " + config.getRedisHost() + ":" + config.getRedisPort());
    }

    /**
     * Request a lock release from the server currently holding it.
     * Broadcasts a {@link LockRequestMessage} to all servers.
     *
     * @param uuid the player UUID whose lock we need
     */
    public void requestLockRelease(UUID uuid) {
        MessageBroker<ByteBuf> b = broker;
        if (b == null) {
            return;
        }
        try {
            // publishOneWay publishes asynchronously (Lettuce async publish),
            // so this returns immediately and never blocks the caller.
            b.publishOneWay(new LockRequestMessage(uuid.toString()), "");
            if (config.isDebug()) {
                logger.info("[Redis] Published REQUEST for " + uuid);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Redis] Failed to publish REQUEST for " + uuid, e);
        }
    }

    /**
     * Notify other servers that a lock has been released.
     * Broadcasts a {@link LockReleasedMessage} to all servers.
     *
     * @param uuid the player UUID whose lock was released
     */
    public void notifyLockReleased(UUID uuid) {
        MessageBroker<ByteBuf> b = broker;
        if (b == null) {
            return;
        }
        try {
            b.publishOneWay(new LockReleasedMessage(uuid.toString()), "");
            if (config.isDebug()) {
                logger.info("[Redis] Published RELEASED for " + uuid);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Redis] Failed to publish RELEASED for " + uuid, e);
        }
    }

    /**
     * Wait for a lock release notification for a specific player.
     *
     * <p>Registers a {@link CountDownLatch}, broadcasts a lock request and then
     * blocks until either the RELEASED message arrives or the timeout elapses.
     *
     * @param uuid      the player UUID to wait for
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if a RELEASED notification was received, false on timeout
     */
    public boolean waitForLockRelease(UUID uuid, long timeoutMs) {
        CountDownLatch latch = new CountDownLatch(1);
        releaseWaiters.put(uuid, latch);

        // Ask the current lock holder to notify us when it is done.
        requestLockRelease(uuid);

        try {
            boolean received = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (received && config.isDebug()) {
                logger.info("[Redis] Received RELEASED notification for " + uuid);
            }
            return received;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            releaseWaiters.remove(uuid);
        }
    }

    /**
     * Stop listening and close the Redis connection.
     */
    public void close() {
        if (broker != null) {
            try {
                broker.unsubscribe();
            } catch (Exception e) {
                logger.log(Level.WARNING, "[Redis] Error unsubscribing broker", e);
            }
            try {
                broker.connection().close();
            } catch (Exception e) {
                logger.log(Level.WARNING, "[Redis] Error closing connection", e);
            }
            broker = null;
            logger.info("Redis connection closed.");
        }
    }

    /**
     * Check if Redis is connected and healthy.
     *
     * @return true if the broker connection is open
     */
    public boolean isHealthy() {
        MessageBroker<ByteBuf> b = broker;
        if (b == null) {
            return false;
        }
        try {
            return b.connection().isOpen();
        } catch (Exception e) {
            return false;
        }
    }

    // ==================== Message callbacks ====================
    //
    // These are invoked by the broker on the receiving side (via the message
    // classes' handle() methods). They are static because the message classes
    // need a stable entry point back into the manager; they route through the
    // singleton {@link #instance}.

    /**
     * Called when a {@link LockRequestMessage} is received.
     *
     * <p>The lock holder cannot release immediately (it is still saving), so we
     * only log the request here. The actual {@link LockReleasedMessage} will be
     * sent automatically when {@code saveData()} completes.
     *
     * @param uuidStr the player UUID whose lock is being requested
     */
    public static void handleLockRequest(String uuidStr) {
        RedisManager inst = instance;
        if (inst == null) {
            return;
        }
        if (inst.config.isDebug()) {
            inst.logger.info("[Redis] Lock release requested for " + uuidStr +
                " (will notify on save completion)");
        }
    }

    /**
     * Called when a {@link LockReleasedMessage} is received.
     *
     * <p>Counts down the latch of any server currently waiting for this UUID so
     * it can retry acquiring the lock immediately.
     *
     * @param uuidStr the player UUID whose lock was released
     */
    public static void handleLockReleased(String uuidStr) {
        RedisManager inst = instance;
        if (inst == null) {
            return;
        }
        try {
            UUID uuid = UUID.fromString(uuidStr);
            CountDownLatch latch = inst.releaseWaiters.get(uuid);
            if (latch != null) {
                latch.countDown();
            }
        } catch (Exception e) {
            inst.logger.log(Level.WARNING, "[Redis] Error handling lock released for " + uuidStr, e);
        }
    }
}
