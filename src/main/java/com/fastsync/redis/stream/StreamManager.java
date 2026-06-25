package com.fastsync.redis.stream;

import com.fastsync.config.ConfigManager;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.models.stream.ClaimedMessages;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages critical sync events via Redis Streams.
 *
 * <p>Redis Streams are an append-only log with consumer groups, providing
 * <strong>recoverable</strong> event delivery — fundamentally different from
 * Pub/Sub (fire-and-forget). Key mechanisms:
 *
 * <ul>
 *   <li><b>XADD</b> — append event to stream (never lost, persists in Redis)
 *   <li><b>XREADGROUP</b> — consumer reads events assigned to it by the group
 *   <li><b>XACK</b> — consumer acknowledges successful processing
 *   <li><b>XPENDING</b> — list unacknowledged events (consumer crashed mid-processing)
 *   <li><b>XAUTOCLAIM</b> — reclaim stale pending entries from crashed consumers
 * </ul>
 *
 * <p>Recovery scenario: Server B crashes while processing a PLAYER_CHECKOUT event.
 * The event remains in the PEL (pending entries list). When Server B restarts
 * (or another server runs XAUTOCLAIM), the event is reclaimed and reprocessed.
 * With Pub/Sub, this event would be permanently lost.
 *
 * <p>Architecture layering:
 * <ul>
 *   <li><b>Pub/Sub</b> (RedisManager) — fast, non-critical lock notifications
 *   <li><b>Streams</b> (this class) — reliable, critical handoff events
 *   <li><b>DB</b> (DatabaseManager) — final source of truth
 * </ul>
 */
public class StreamManager {

    private static final String STREAM_KEY = "fastsync:stream:events";
    private static final String CONSUMER_GROUP = "fastsync-group";
    private static final long BLOCK_MS = 2000; // XREADGROUP block timeout
    private static final long AUTODAIM_IDLE_MS = 30000; // reclaim entries idle > 30s
    private static final int MAX_RECLAIM_PER_CYCLE = 10;

    private final Logger logger;
    private final ConfigManager config;
    private final String serverName;
    private final List<StreamEventListener> listeners = new ArrayList<>();

    private StatefulRedisConnection<String, String> connection;
    private RedisStreamCommands<String, String> streamCommands;
    private Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Track events we've published (for dedup / correlation)
    private final ConcurrentHashMap<String, String> publishedEventIds = new ConcurrentHashMap<>();

    public StreamManager(Logger logger, ConfigManager config, String serverName) {
        this.logger = logger;
        this.config = config;
        this.serverName = serverName;
    }

    /**
     * Initialize the stream connection, create consumer group, and start
     * the background consumer thread.
     */
    public void initialize() {
        String password = config.getRedisPassword();
        String scheme = config.isRedisSsl() ? "rediss://" : "redis://";
        String auth = (password != null && !password.isEmpty()) ? ":" + password + "@" : "";
        String redisUri = scheme + auth + config.getRedisHost() + ":" + config.getRedisPort()
            + "/" + config.getRedisDatabase();

        RedisClient client = RedisClient.create(redisUri);
        connection = client.connect();
        streamCommands = connection.sync();

        // Create consumer group (ignore "BUSYGROUP" = already exists)
        try {
            streamCommands.xgroupCreate(XReadArgs.StreamOffset.from(STREAM_KEY, "0"),
                CONSUMER_GROUP, XGroupCreateArgs.Builder.mkstream(true));
            logger.info("[Stream] Consumer group created: " + CONSUMER_GROUP);
        } catch (RedisCommandExecutionException e) {
            if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                logger.info("[Stream] Consumer group already exists: " + CONSUMER_GROUP);
            } else {
                throw e;
            }
        }

        // Recover any pending entries from a previous crash
        recoverPendingEntries();

        // Start background consumer
        running.set(true);
        consumerThread = new Thread(this::consumeLoop, "FastSync-Stream-Consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Publish server start event
        publish(StreamEvent.create(StreamEventType.SERVER_START, null, serverName,
            "", 0, 0, "Server started"));

        logger.info("[Stream] Redis Streams initialized (key=" + STREAM_KEY + ").");
    }

    /**
     * Publish a critical event to the stream.
     * Returns immediately (XADD is fast, ~O(1)).
     */
    public void publish(StreamEvent event) {
        if (streamCommands == null) return;
        try {
            String id = streamCommands.xadd(STREAM_KEY, event.toMap());
            publishedEventIds.put(event.type().name() + ":" + event.uuid(), id);
            if (config.isDebug()) {
                logger.info("[Stream] Published " + event.type() + " (id=" + id +
                    ", uuid=" + event.uuid() + ")");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Stream] Failed to publish " + event.type(), e);
        }
    }

    /**
     * Register a listener for stream events.
     */
    public void addListener(StreamEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Main consumer loop: XREADGROUP → dispatch → XACK.
     * Runs on a daemon thread.
     */
    private void consumeLoop() {
        while (running.get()) {
            try {
                // Read new messages for this consumer (">" = never delivered)
                List<StreamMessage<String, String>> messages = streamCommands.xreadgroup(
                    Consumer.from(CONSUMER_GROUP, serverName),
                    XReadArgs.Builder.block(BLOCK_MS).count(10),
                    XReadArgs.StreamOffset.lastConsumed(STREAM_KEY)
                );

                if (messages == null || messages.isEmpty()) continue;

                for (StreamMessage<String, String> msg : messages) {
                    handleStreamMessage(msg);
                    // Acknowledge after successful processing
                    streamCommands.xack(STREAM_KEY, CONSUMER_GROUP, msg.getId());
                }
            } catch (Exception e) {
                if (running.get()) {
                    logger.log(Level.WARNING, "[Stream] Consumer loop error", e);
                    try { Thread.sleep(1000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        logger.info("[Stream] Consumer loop stopped.");
    }

    /**
     * Handle a single stream message by dispatching to listeners.
     */
    private void handleStreamMessage(StreamMessage<String, String> msg) {
        try {
            StreamEvent event = StreamEvent.fromMap(msg.getId(), msg.getBody());

            // Skip our own published events (we published them, don't re-process)
            if (serverName.equals(event.server())) {
                if (config.isDebug()) {
                    logger.info("[Stream] Skipping own event: " + event.type() + " (id=" + event.id() + ")");
                }
                return;
            }

            if (config.isDebug()) {
                logger.info("[Stream] Received " + event.type() + " from " + event.server() +
                    " (id=" + event.id() + ", uuid=" + event.uuid() + ")");
            }

            for (StreamEventListener listener : listeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "[Stream] Listener error for " + event.type(), e);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Stream] Failed to handle message " + msg.getId(), e);
        }
    }

    /**
     * Recover pending entries from a previous crash.
     *
     * <p>When this server crashed and restarted, its previously delivered but
     * unacknowledged events are still in the PEL. We reclaim them with
     * XAUTOCLAIM and reprocess.
     */
    private void recoverPendingEntries() {
        try {
            // XAUTOCLAIM: reclaim entries that have been idle for too long
            // (e.g., from a previous crash of this consumer)
            XAutoClaimArgs<String> autoclaimArgs = new XAutoClaimArgs<String>()
                .consumer(Consumer.from(CONSUMER_GROUP, serverName))
                .minIdleTime(AUTODAIM_IDLE_MS)
                .startId("0-0")
                .count(MAX_RECLAIM_PER_CYCLE);
            ClaimedMessages<String, String> claimed = streamCommands.xautoclaim(
                STREAM_KEY, autoclaimArgs);
            List<StreamMessage<String, String>> reclaimed = claimed.getMessages();

            if (reclaimed != null && !reclaimed.isEmpty()) {
                logger.info("[Stream] Recovered " + reclaimed.size() +
                    " pending entries from previous crash.");
                for (StreamMessage<String, String> msg : reclaimed) {
                    handleStreamMessage(msg);
                    streamCommands.xack(STREAM_KEY, CONSUMER_GROUP, msg.getId());
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Stream] Failed to recover pending entries", e);
        }
    }

    /**
     * Get stream statistics for monitoring.
     */
    public void logStats() {
        if (streamCommands == null) return;
        try {
            long length = streamCommands.xlen(STREAM_KEY);
            logger.info("[Stream] Stream length: " + length + " entries, " +
                "published (this session): " + publishedEventIds.size());
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Stream] Failed to get stats", e);
        }
    }

    /**
     * Shutdown the stream consumer and close connection.
     */
    public void close() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Publish server stop event
        publish(StreamEvent.create(StreamEventType.SERVER_STOP, null, serverName,
            "", 0, 0, "Server shutting down"));

        if (connection != null) {
            connection.close();
            logger.info("[Stream] Connection closed.");
        }
    }

    public boolean isHealthy() {
        return connection != null && connection.isOpen();
    }
}
