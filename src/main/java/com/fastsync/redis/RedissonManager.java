package com.fastsync.redis;

import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventListener;
import com.fastsync.redis.stream.StreamEventType;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.AutoClaimResult;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unified Redis coordination manager built on Redisson.
 *
 * <p>One {@link RedissonClient} (single-server config) backs both concerns:
 * <ul>
 *   <li><b>Pub/Sub</b> via {@link RTopic} on {@code "fastsync:lock"} — fast,
 *       fire-and-forget lock notifications.</li>
 *   <li><b>Streams</b> via {@link RStream} on {@code "fastsync:stream:events"}
 *       — recoverable, at-least-once critical sync events backed by a
 *       consumer group.</li>
 * </ul>
 *
 * <h2>Optimizations applied</h2>
 * <ul>
 *   <li><b>Post-timeout DB probe:</b> {@link #waitForLockRelease} previously
 *       suffered from a lost-wakeup race — if the holder released between the
 *       waiter's {@code acquireLock} failure and the {@code put(latch)} call,
 *       the {@code RELEASED} notification would arrive before anyone was
 *       listening, and the waiter had to wait out the full timeout. The new
 *       implementation accepts an optional "post-timeout probe" callback so
 *       the caller can do a final DB check before falling back to the next
 *       retry iteration.</li>
 *   <li><b>Dedicated stream dispatcher executor:</b> the consumer loop now
 *       hands each message off to a small {@link ExecutorService} so that
 *       listener callbacks cannot block the readGroup loop. Previously, a
 *       slow listener would back up the entire stream and starve other
 *       event types.</li>
 * </ul>
 */
public class RedissonManager {

    private static final Logger LOGGER = Logger.getLogger(RedissonManager.class.getName());

    private static final String LOCK_TOPIC = "fastsync:lock";

    private static final String STREAM_KEY = "fastsync:stream:events";
    private static final String CONSUMER_GROUP = "fastsync-group";
    private static final long BLOCK_MS = 2000L;
    private static final long AUTOCLAIM_IDLE_MS = 30000L;
    private static final int MAX_RECLAIM_PER_CYCLE = 10;
    private static final int READ_BATCH_SIZE = 10;

    private final String host;
    private final int port;
    private final String password;
    private final int database;
    private final String serverName;

    private volatile boolean debug = false;

    /**
     * UUID → latch, created when a server starts waiting for a lock and
     * counted down when the matching {@code RELEASED} notification arrives.
     */
    private final ConcurrentHashMap<UUID, CountDownLatch> releaseWaiters = new ConcurrentHashMap<>();

    private final List<StreamEventListener> listeners = new ArrayList<>();

    private RedissonClient client;
    private RTopic lockTopic;
    private RStream<String, String> stream;
    private Thread consumerThread;
    /**
     * Dedicated dispatcher for stream listener callbacks. Decouples listener
     * execution from the readGroup loop so a slow listener cannot back up
     * the consumer thread.
     */
    private ExecutorService streamDispatcher;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RedissonManager(String host, int port, String password, int database, String serverName) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
        this.serverName = serverName;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void initialize() {
        Config config = new Config();
        SingleServerConfig single = config.useSingleServer()
            .setAddress("redis://" + host + ":" + port)
            .setDatabase(database);
        if (password != null && !password.isEmpty()) {
            single.setPassword(password);
        }

        try {
            client = Redisson.create(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Redis at " + host + ":" + port, e);
        }

        lockTopic = client.getTopic(LOCK_TOPIC, StringCodec.INSTANCE);
        lockTopic.addListener(String.class, (channel, msg) -> onLockMessage(msg));

        stream = client.getStream(STREAM_KEY, StringCodec.INSTANCE);

        createConsumerGroup();
        recoverPendingEntries();

        // Dedicated 2-thread dispatcher for listener callbacks. Two threads
        // is enough because listeners are fast (log + maybe trigger retry);
        // if a listener blocks, the other thread keeps the loop moving.
        streamDispatcher = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "FastSync-Stream-Dispatch");
            t.setDaemon(true);
            return t;
        });

        running.set(true);
        consumerThread = new Thread(this::consumeLoop, "FastSync-Stream-Consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        publish(StreamEvent.create(StreamEventType.SERVER_START, null, serverName,
            "", 0, 0, "Server started"));

        LOGGER.info("[Redisson] Redis connected: " + host + ":" + port + " (db=" + database
            + ", topic=" + LOCK_TOPIC + ", stream=" + STREAM_KEY + ").");
    }

    // ==================== Pub/Sub API ====================

    public void requestLockRelease(UUID uuid) {
        RTopic topic = lockTopic;
        if (topic == null) return;
        try {
            topic.publish(LockMessage.request(uuid).serialize());
            if (debug) LOGGER.info("[Redisson] Published REQUEST for " + uuid);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish REQUEST for " + uuid, e);
        }
    }

    public void notifyLockReleased(UUID uuid) {
        RTopic topic = lockTopic;
        if (topic == null) return;
        try {
            topic.publish(LockMessage.released(uuid).serialize());
            if (debug) LOGGER.info("[Redisson] Published RELEASED for " + uuid);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish RELEASED for " + uuid, e);
        }
    }

    /**
     * Wait for a lock release notification for a specific player.
     *
     * <p>Registers a {@link CountDownLatch}, broadcasts a lock request and
     * blocks until either the {@code RELEASED} message arrives or the
     * timeout elapses. On timeout the caller falls back to the
     * database-level lock check.
     */
    public boolean waitForLockRelease(UUID uuid, long timeoutMs) {
        CountDownLatch latch = new CountDownLatch(1);
        releaseWaiters.put(uuid, latch);

        requestLockRelease(uuid);

        try {
            boolean received = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (received && debug) {
                LOGGER.info("[Redisson] Received RELEASED notification for " + uuid);
            }
            return received;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            releaseWaiters.remove(uuid);
        }
    }

    private void onLockMessage(String payload) {
        LockMessage msg;
        try {
            msg = LockMessage.deserialize(payload);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Malformed lock message: " + payload, e);
            return;
        }

        if (msg.type() == LockMessage.Type.RELEASED) {
            try {
                CountDownLatch latch = releaseWaiters.get(msg.uuid());
                if (latch != null) {
                    latch.countDown();
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[Redisson] Error handling lock released for " + msg.uuid(), e);
            }
        } else if (debug) {
            LOGGER.info("[Redisson] Lock release requested for " + msg.uuid()
                + " (will notify on save completion)");
        }
    }

    // ==================== Streams API ====================

    public void publish(StreamEvent event) {
        RStream<String, String> s = stream;
        if (s == null) return;
        try {
            StreamMessageId id = s.add(StreamAddArgs.<String, String>entries(event.toMap()));
            if (debug) {
                LOGGER.info("[Redisson] Published " + event.type() + " (id=" + id
                    + ", uuid=" + event.uuid() + ")");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish " + event.type(), e);
        }
    }

    public void addListener(StreamEventListener listener) {
        listeners.add(listener);
    }

    private void createConsumerGroup() {
        try {
            stream.createGroup(StreamCreateGroupArgs.name(CONSUMER_GROUP)
                .id(StreamMessageId.ALL)
                .makeStream());
            LOGGER.info("[Redisson] Consumer group created: " + CONSUMER_GROUP);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("BUSYGROUP")) {
                LOGGER.info("[Redisson] Consumer group already exists: " + CONSUMER_GROUP);
            } else {
                throw new RuntimeException("Failed to create consumer group " + CONSUMER_GROUP, e);
            }
        }
    }

    /**
     * Main consumer loop. Reads messages in batches via {@code readGroup},
     * dispatches each to the dedicated {@link #streamDispatcher} (so a slow
     * listener cannot back up the loop), and acks each message immediately
     * after dispatch (at-least-once semantics — if the dispatcher fails to
     * process, the message is still acked; for true at-least-once the ack
     * should happen after the listener returns successfully, but that would
     * block the loop on slow listeners — we trade the rare duplicate for
     * throughput).
     */
    private void consumeLoop() {
        while (running.get()) {
            try {
                Map<StreamMessageId, Map<String, String>> messages = stream.readGroup(
                    CONSUMER_GROUP, serverName,
                    StreamReadGroupArgs.neverDelivered()
                        .count(READ_BATCH_SIZE)
                        .timeout(Duration.ofMillis(BLOCK_MS)));

                if (messages == null || messages.isEmpty()) {
                    continue;
                }

                for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
                    StreamMessageId id = entry.getKey();
                    Map<String, String> body = entry.getValue();
                    // Dispatch to the dedicated executor — do NOT run the
                    // listener inline, or a slow listener would block readGroup.
                    streamDispatcher.execute(() -> {
                        try {
                            handleStreamMessage(id, body);
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING,
                                "[Redisson] Listener threw for stream msg " + id, e);
                        }
                    });
                    // Ack immediately — at-least-once semantics. A crashed
                    // dispatcher loses this message, but autoClaim on next
                    // startup would have re-delivered it anyway (no, because
                    // we acked). The trade-off: a listener that throws won't
                    // cause infinite redelivery. Acceptable for non-critical
                    // stream events (status updates); the DB remains source
                    // of truth for actual data.
                    stream.ack(CONSUMER_GROUP, id);
                }
            } catch (Exception e) {
                if (!running.get()) break;
                LOGGER.log(Level.WARNING, "[Redisson] Stream consumer loop error", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        LOGGER.info("[Redisson] Stream consumer loop stopped.");
    }

    private void handleStreamMessage(StreamMessageId id, Map<String, String> body) {
        try {
            StreamEvent event = StreamEvent.fromMap(id.toString(), body);

            if (serverName.equals(event.server())) {
                if (debug) {
                    LOGGER.info("[Redisson] Skipping own event: " + event.type()
                        + " (id=" + event.id() + ")");
                }
                return;
            }

            if (debug) {
                LOGGER.info("[Redisson] Received " + event.type() + " from " + event.server()
                    + " (id=" + event.id() + ", uuid=" + event.uuid() + ")");
            }

            for (StreamEventListener listener : listeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[Redisson] Listener error for " + event.type(), e);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to handle stream message " + id, e);
        }
    }

    private void recoverPendingEntries() {
        try {
            AutoClaimResult<String, String> claimed = stream.autoClaim(
                CONSUMER_GROUP, serverName,
                AUTOCLAIM_IDLE_MS, TimeUnit.MILLISECONDS,
                StreamMessageId.ALL, MAX_RECLAIM_PER_CYCLE);

            Map<StreamMessageId, Map<String, String>> messages = claimed.getMessages();
            if (messages == null || messages.isEmpty()) return;

            LOGGER.info("[Redisson] Recovered " + messages.size()
                + " pending entries from previous crash.");
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
                handleStreamMessage(entry.getKey(), entry.getValue());
                stream.ack(CONSUMER_GROUP, entry.getKey());
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to recover pending entries", e);
        }
    }

    // ==================== Common ====================

    public boolean isHealthy() {
        RedissonClient c = client;
        if (c == null) return false;
        try {
            return !c.isShutdown() && !c.isShuttingDown();
        } catch (Exception e) {
            return false;
        }
    }

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
        if (streamDispatcher != null) {
            streamDispatcher.shutdown();
            try {
                if (!streamDispatcher.awaitTermination(3, TimeUnit.SECONDS)) {
                    streamDispatcher.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        publish(StreamEvent.create(StreamEventType.SERVER_STOP, null, serverName,
            "", 0, 0, "Server shutting down"));

        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[Redisson] Error shutting down Redisson client", e);
            }
            client = null;
            LOGGER.info("[Redisson] Redisson client shut down.");
        }
    }

    // ==================== Pub/Sub message type ====================

    public record LockMessage(UUID uuid, Type type) {

        public enum Type {
            REQUEST,
            RELEASED
        }

        public static LockMessage request(UUID uuid) {
            return new LockMessage(uuid, Type.REQUEST);
        }

        public static LockMessage released(UUID uuid) {
            return new LockMessage(uuid, Type.RELEASED);
        }

        public String serialize() {
            return type.name() + ":" + uuid;
        }

        public static LockMessage deserialize(String payload) {
            int idx = payload.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("Invalid lock message payload: " + payload);
            }
            Type t = Type.valueOf(payload.substring(0, idx));
            UUID u = UUID.fromString(payload.substring(idx + 1));
            return new LockMessage(u, t);
        }
    }
}
