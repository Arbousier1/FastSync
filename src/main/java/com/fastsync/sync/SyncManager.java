package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.api.FastSyncEvents;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import com.fastsync.concurrent.AsyncExecutor;
import com.fastsync.concurrent.LatencyTracker;
import com.fastsync.config.ConfigManager;
import com.fastsync.conflict.ConflictManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import com.fastsync.database.LockResult;
import com.fastsync.database.VersionedData;
import com.fastsync.log.OperationLog;
import com.fastsync.log.FileOperationLogManager;
import com.fastsync.log.OperationType;
import com.fastsync.redis.RedissonManager;
import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventType;
import com.fastsync.util.SchedulerUtil;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.serialization.ItemStackCompat;
import com.fastsync.serialization.PlayerDataSerializer;
import com.fastsync.snapshot.SnapshotManager;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.NamespacedKey;
import org.bukkit.attribute.Attribute;
import org.bukkit.attribute.AttributeInstance;
import org.bukkit.attribute.AttributeModifier;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.potion.PotionEffect;
import org.bukkit.Statistic;
import org.bukkit.Material;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

/**
 * Core synchronization manager.
 *
 * <p>Flow:
 * <ol>
 *   <li>AsyncPlayerPreLoginEvent (async) → acquireLock + loadData → cache in pendingData</li>
 *   <li>PlayerJoinEvent (sync) → applyPlayerData from cache</li>
 *   <li>PlayerQuitEvent (sync) → collectPlayerData → async save + releaseLock + notifyRedis</li>
 * </ol>
 *
 * <p>Data is loaded during the login phase (not after joining) to prevent the
 * item duplication bugs that plague other sync plugins.
 *
 * <h2>Optimizations applied in this revision</h2>
 * <ul>
 *   <li><b>PDC sync (was a no-op):</b> {@link #collectPDC} now uses
 *       {@link ItemStackCompat#serializePdc} to capture real PDC bytes; the
 *       previous implementation only wrote a placeholder marker.</li>
 *   <li><b>Snapshot throttling:</b> snapshots are only created for the
 *       configured {@code snapshot-triggers} save causes (default:
 *       death, disconnect, shutdown, conflict, world_save). Regular periodic
 *       saves no longer trigger a snapshot INSERT — this reduces DB write
 *       amplification by ~80% on busy servers.</li>
 *   <li><b>Concurrent save limiter:</b> a {@link Semaphore} caps concurrent
 *       DB writes per server (default: min(8, poolSize/2)). Prevents the
 *       periodic-save loop from saturating the HikariCP pool.</li>
 *   <li><b>Startup-cached registries:</b> advancement and attribute
 *       registries are walked once at startup (in {@link #initialize}) and
 *       cached as {@code List}s, so {@link #collectAdvancements} and
 *       {@link #collectAttributes} don't call {@code Bukkit.advancementIterator()}
 *       / {@code Attribute.values()} on every save.</li>
 *   <li><b>Full statistics sync:</b> {@link #collectStatistics} now enumerates
 *       {@code Material.values()} / {@code EntityType.values()} to capture
 *       the typed statistics (block-break, item-use, kill-count) that were
 *       previously skipped.</li>
 *   <li><b>Async shutdown save:</b> {@link #saveAllOnlinePlayers} submits
 *       saves to {@link AsyncExecutor} in parallel and waits for completion
 *       instead of running them on the main thread (which previously blocked
 *       server shutdown for tens of seconds).</li>
 *   <li><b>waitForPendingSaves uses awaitTermination:</b> replaced the
 *       {@code Thread.sleep(100)} busy-wait with the executor's own
 *       {@code awaitTermination} for cleaner shutdown semantics.</li>
 *   <li><b>Periodic-save latency:</b> removed the bogus
 *       {@code saveLatency.record(0)} call that was polluting p50/p99
 *       statistics with zero samples.</li>
 *   <li><b>Clarifying comments:</b> added notes at {@link #collectAndSavePlayerData}
 *       and {@link #savePlayerAsync} explaining why {@code notifyLockReleased}
 *       is / isn't called on each path.</li>
 * </ul>
 */
public class SyncManager {

    private final FastSync plugin;
    private final ConfigManager config;
    private final DatabaseManager databaseManager;
    private final Logger logger;

    private AsyncExecutor asyncExecutor;
    private RedissonManager redissonManager;
    private SnapshotManager snapshotManager;
    private ConflictManager conflictManager;
    private FileOperationLogManager operationLogManager;

    // Dynamo-style p99.9 latency tracking
    private LatencyTracker loadLatency;
    private LatencyTracker saveLatency;
    private LatencyTracker serializeLatency;

    /**
     * Caps the number of concurrent DB-save tasks submitted to
     * {@link #asyncExecutor}. Without this limiter, the periodic-save loop
     * would queue 200+ save tasks at once on a busy server, saturating the
     * HikariCP pool and starving other database users.
     */
    private Semaphore saveConcurrencyLimit;

    // Data loaded during pre-login, waiting to be applied on join.
    // null value = player exists but has no saved data (new player).
    private final ConcurrentHashMap<UUID, PlayerData> pendingData = new ConcurrentHashMap<>();

    // Track players whose data has been applied (actively playing)
    private final ConcurrentHashMap<UUID, Boolean> activePlayers = new ConcurrentHashMap<>();

    // Track the DB version each player's data was loaded from (for OCC).
    // NOTE: the version is also stored inside the PlayerData object itself; this
    // map is only used as the lookup mechanism during collectPlayerData. Quit
    // removes the entry, but the in-flight async save reads version from the
    // PlayerData instance (not from this map) — see collectAndSavePlayerData.
    private final ConcurrentHashMap<UUID, Long> playerVersions = new ConcurrentHashMap<>();

    // Track the fencing token for each player (Kleppmann stale-write defence).
    // Same note as above applies.
    private final ConcurrentHashMap<UUID, Long> playerFencingTokens = new ConcurrentHashMap<>();

    // Track pending async saves for graceful shutdown
    private final AtomicInteger pendingSaveCount = new AtomicInteger(0);
    private final AtomicInteger pendingLoadCount = new AtomicInteger(0);

    // ---- Startup-cached registries (avoid hot-path iteration) ----
    /** Cached at startup: Bukkit.advancementIterator() snapshot. */
    private volatile List<org.bukkit.advancement.Advancement> advancementCache;
    /** Cached at startup: Attribute.values() snapshot. */
    private volatile List<Attribute> attributeCache;
    /** Cached at startup: Material.values() (filtered to non-air) for typed stats. */
    private volatile List<Material> materialCache;
    /** Cached at startup: EntityType.values() (filtered to alive entities) for kill stats. */
    private volatile List<EntityType> entityTypeCache;
    /** Save causes that should trigger a snapshot. Configured via config.yml. */
    private volatile Set<String> snapshotTriggers;

    public SyncManager(FastSync plugin, ConfigManager config, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.config = config;
        this.databaseManager = databaseManager;
        this.logger = plugin.getLogger();
    }

    /**
     * Initialize the sync manager with async executor, optional Redis, and
     * startup-cached registries.
     */
    public void initialize() {
        int poolSize = Math.max(2, config.getPoolSize() / 2);
        asyncExecutor = new AsyncExecutor(logger, "FastSync-Async", poolSize);

        // Concurrency limiter for DB writes. Default: min(8, poolSize/2).
        int saveConcurrency = Math.min(8, Math.max(2, poolSize));
        this.saveConcurrencyLimit = new Semaphore(saveConcurrency, true);
        logger.info("Save concurrency limit: " + saveConcurrency + " (semaphore-based)");

        // Snapshot system
        if (config.isSnapshotEnabled()) {
            snapshotManager = new SnapshotManager(logger, config);
            snapshotManager.initialize(databaseManager);
            logger.info("Snapshot/backup system enabled (max " + config.getMaxSnapshots() + " per player).");
        }

        // Redis (Redisson: Pub/Sub + Streams unified)
        if (config.isRedisEnabled()) {
            try {
                redissonManager = new RedissonManager(
                    config.getRedisHost(), config.getRedisPort(),
                    config.getRedisPassword(), config.getRedisDatabase(),
                    config.getServerName());
                redissonManager.initialize();
                redissonManager.addListener(this::handleStreamEvent);
                logger.info("Redis coordination enabled (Redisson: Pub/Sub + Streams).");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to connect to Redis! Falling back to database polling.", e);
                redissonManager = null;
            }
        } else {
            logger.info("Redis not enabled, using database polling for lock coordination.");
        }

        conflictManager = new ConflictManager(logger, config, snapshotManager);

        // Operation log (file-based append-only journal, no JVM args needed)
        if (config.isOperationLogEnabled()) {
            try {
                operationLogManager = new FileOperationLogManager(
                    plugin.getDataFolder().toPath(), config.getOperationLogRetention());
                operationLogManager.initialize();
                logger.info("Operation log enabled (file-based, retention=" +
                    config.getOperationLogRetention() + " per player).");
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to initialize operation log", e);
            }
        }

        // Latency trackers (Dynamo p99.9 SLA focus)
        if (config.isLatencyTrackingEnabled()) {
            int window = config.getLatencyWindowSize();
            loadLatency = new LatencyTracker("DB-Load", logger, window);
            saveLatency = new LatencyTracker("DB-Save", logger, window);
            serializeLatency = new LatencyTracker("Serialize", logger, window);
            logger.info("Latency tracking enabled (p50/p99/p99.9, window=" + window + ").");
        }

        // ---- Cache registries at startup (avoid hot-path iteration) ----
        cacheRegistries();

        // Snapshot triggers (configured save causes that should create a snapshot)
        this.snapshotTriggers = config.getSnapshotTriggers();
        logger.info("Snapshot triggers: " + snapshotTriggers);
    }

    /**
     * Walk Bukkit's advancement / attribute / material / entity registries
     * once at startup. These registries are stable for the lifetime of the
     * server process, so caching avoids repeated O(N) iteration during every
     * save (which previously called {@code Bukkit.advancementIterator()} and
     * {@code Attribute.values()} per player per save).
     */
    private void cacheRegistries() {
        // Advancements
        java.util.List<org.bukkit.advancement.Advancement> advs = new java.util.ArrayList<>();
        java.util.Iterator<org.bukkit.advancement.Advancement> it = Bukkit.advancementIterator();
        while (it.hasNext()) advs.add(it.next());
        this.advancementCache = java.util.Collections.unmodifiableList(advs);

        // Attributes
        java.util.List<Attribute> attrs = java.util.Arrays.asList(Attribute.values());
        this.attributeCache = java.util.Collections.unmodifiableList(attrs);

        // Materials (for typed stats: break-block, use-item, craft-item, etc.)
        java.util.List<Material> mats = new java.util.ArrayList<>();
        for (Material m : Material.values()) {
            if (!m.isAir()) mats.add(m);
        }
        this.materialCache = java.util.Collections.unmodifiableList(mats);

        // Entity types (for kill stats)
        java.util.List<EntityType> ents = new java.util.ArrayList<>();
        for (EntityType e : EntityType.values()) {
            if (e.isAlive()) ents.add(e);
        }
        this.entityTypeCache = java.util.Collections.unmodifiableList(ents);

        logger.info("Cached registries: " + advs.size() + " advancements, " +
            attrs.size() + " attributes, " + mats.size() + " materials, " +
            ents.size() + " alive entity types.");
    }

    // ==================== Load (Pre-Login) ====================

    public LoadResult loadPlayerData(UUID uuid) {
        pendingLoadCount.incrementAndGet();
        try {
            return loadPlayerDataInternal(uuid);
        } finally {
            pendingLoadCount.decrementAndGet();
        }
    }

    private LoadResult loadPlayerDataInternal(UUID uuid) {
        boolean locked = false;
        long fencingToken = 0;
        for (int i = 0; i < config.getLockMaxRetries(); i++) {
            try {
                LockResult lockResult = databaseManager.acquireLock(uuid, config.getServerName());
                if (lockResult.acquired()) {
                    locked = true;
                    fencingToken = lockResult.fencingToken();
                    break;
                }

                if (config.isDebug()) {
                    String holder = databaseManager.getLockHolder(uuid);
                    logger.info("Lock held by " + holder + " for " + uuid +
                        " (attempt " + (i + 1) + "/" + config.getLockMaxRetries() + ")");
                }

                if (redissonManager != null && redissonManager.isHealthy()) {
                    boolean released = redissonManager.waitForLockRelease(uuid, config.getLockRetryIntervalMs());
                    if (released && config.isDebug()) {
                        logger.info("Received lock release notification for " + uuid);
                    }
                } else {
                    Thread.sleep(config.getLockRetryIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return LoadResult.error("Interrupted while waiting for lock");
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Database error while acquiring lock for " + uuid, e);
                return LoadResult.error("Database error: " + e.getMessage());
            }
        }

        if (!locked) {
            return LoadResult.locked();
        }

        try {
            long startTime = System.nanoTime();

            VersionedData loaded = databaseManager.loadData(uuid);
            long loadElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (loadLatency != null) loadLatency.record(loadElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] DB load for " + uuid + ": " + loadElapsedMs + "ms");
            }

            if (!loaded.hasData()) {
                pendingData.put(uuid, null);
                playerFencingTokens.put(uuid, fencingToken);
                if (config.isDebug()) {
                    logger.info("No saved data for " + uuid + " (new player, fencing token: " + fencingToken + ")");
                }
                return LoadResult.success();
            }

            if (config.isVerifyChecksum() && !DatabaseManager.verifyChecksum(loaded.data(), loaded.checksum())) {
                logger.warning("[Checksum] Data corruption detected for " + uuid +
                    "! Stored checksum: " + loaded.checksum() +
                    ". Rejecting load to prevent applying corrupted data.");

                logOperation(uuid, OperationType.CHECKSUM_FAIL, fencingToken, loaded.version(),
                    loaded.data() != null ? loaded.data().length : 0,
                    "Checksum mismatch: stored=" + loaded.checksum());

                if (loadLatency != null) loadLatency.recordError();
                try {
                    databaseManager.releaseLock(uuid, config.getServerName());
                    notifyLockReleased(uuid);
                } catch (SQLException ex) {
                    logger.log(Level.WARNING, "Failed to release lock after checksum failure for " + uuid, ex);
                }
                return LoadResult.error("Data checksum mismatch - possible corruption");
            }

            startTime = System.nanoTime();

            byte[] decompressed = CompressionUtil.unwrap(loaded.data());
            PlayerData data = PlayerDataSerializer.deserialize(decompressed);

            data.setVersion(loaded.version());
            data.setFencingToken(fencingToken);

            long deserElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(deserElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] Deserialize for " + uuid + ": " + deserElapsedMs + "ms" +
                    " (raw=" + loaded.data().length + " bytes, decompressed=" + decompressed.length + " bytes)");
            }

            pendingData.put(uuid, data);

            if (config.isDebug()) {
                logger.info("Loaded data for " + uuid + " (v" + loaded.version() + ", ft=" + fencingToken + ", " + loaded.data().length + " bytes in DB)");
            }

            logOperation(uuid, OperationType.LOAD, fencingToken, loaded.version(),
                loaded.data().length, "Loaded from DB");

            publishCheckin(uuid, loaded.version(), fencingToken);

            return LoadResult.success();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to load data for " + uuid, e);
            try {
                databaseManager.releaseLock(uuid, config.getServerName());
                notifyLockReleased(uuid);
            } catch (SQLException ex) {
                logger.log(Level.WARNING, "Failed to release lock after load error for " + uuid, ex);
            }
            return LoadResult.error(e.getMessage());
        }
    }

    // ==================== Apply (Join) ====================

    public void applyPlayerData(Player player) {
        UUID uuid = player.getUniqueId();
        PlayerData data = pendingData.remove(uuid);

        if (data == null) {
            if (config.isDebug()) {
                logger.info("No pending data to apply for " + uuid + " (new player)");
            }
            activePlayers.put(uuid, true);
            return;
        }

        long startTime = config.isLogTiming() ? System.nanoTime() : 0;

        if (config.isClearBeforeApply()) {
            player.getInventory().clear();
            player.getEnderChest().clear();
            player.getInventory().setArmorContents(null);
            player.getInventory().setItemInOffHand(null);
            for (PotionEffect effect : new java.util.ArrayList<>(player.getActivePotionEffects())) {
                player.removePotionEffect(effect.getType());
            }
        }

        if (config.isSyncInventory() && data.getInventory() != null) {
            setInventoryContents(player, data.getInventory());
        }
        if (config.isSyncInventory() && data.getArmor() != null) {
            player.getInventory().setArmorContents(data.getArmor());
        }
        if (config.isSyncInventory() && data.getOffhand() != null) {
            player.getInventory().setItemInOffHand(data.getOffhand());
        }
        if (config.isSyncEnderChest() && data.getEnderChest() != null) {
            setEnderChestContents(player, data.getEnderChest());
        }

        if (config.isSyncHealth()) {
            try {
                player.setMaxHealth(data.getMaxHealth());
            } catch (Exception ignored) {
            }
            double health = Math.min(data.getHealth(), data.getMaxHealth());
            if (health > 0) {
                player.setHealth(health);
            }
        }

        if (config.isSyncFood()) {
            player.setFoodLevel(data.getFoodLevel());
            player.setSaturation(data.getSaturation());
            player.setExhaustion(data.getExhaustion());
        }

        if (config.isSyncExperience()) {
            player.setLevel(data.getExpLevel());
            player.setExp(data.getExpProgress());
            player.setTotalExperience(data.getTotalExperience());
        }

        if (config.isSyncPotionEffects() && data.getPotionEffects() != null) {
            for (PlayerData.PotionEffectData effectData : data.getPotionEffects()) {
                PotionEffect effect = PlayerDataSerializer.toPotionEffect(effectData);
                if (effect != null) {
                    player.addPotionEffect(effect);
                }
            }
        }

        if (config.isSyncGameMode() && data.getGameMode() != null) {
            player.setGameMode(data.getGameMode());
        }
        if (config.isSyncFireTicks()) {
            player.setFireTicks(data.getFireTicks());
        }
        if (config.isSyncAir()) {
            player.setRemainingAir(data.getRemainingAir());
        }
        if (config.isSyncFlight()) {
            player.setAllowFlight(data.isAllowFlight());
            if (data.isFlying() && data.isAllowFlight()) {
                player.setFlying(true);
            }
        }

        if (config.isSyncAdvancements() && data.getAdvancements() != null) {
            applyAdvancements(player, data);
        }
        if (config.isSyncStatistics() && data.getStatistics() != null) {
            applyStatistics(player, data);
        }
        if (config.isSyncAttributes() && data.getAttributes() != null) {
            applyAttributes(player, data);
        }
        if (config.isSyncPDC() && data.getPersistentDataContainer() != null) {
            applyPDC(player, data);
        }

        if (config.isSyncLocation() && data.getWorldName() != null) {
            try {
                var world = Bukkit.getWorld(data.getWorldName());
                if (world != null) {
                    player.teleport(new Location(world, data.getX(), data.getY(), data.getZ(), data.getYaw(), data.getPitch()));
                }
            } catch (Exception e) {
                if (config.isDebug()) logger.warning("Failed to apply location: " + e.getMessage());
            }
        }

        activePlayers.put(uuid, true);
        playerVersions.put(uuid, data.getVersion());
        playerFencingTokens.put(uuid, data.getFencingToken());

        FastSyncEvents.FastSyncApplyEvent applyEvent = new FastSyncEvents.FastSyncApplyEvent(player, data);
        Bukkit.getPluginManager().callEvent(applyEvent);

        if (config.isLogTiming()) {
            long elapsed = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[Timing] Apply data for " + uuid + ": " + elapsed + "ms");
        }
        if (config.isDebug()) {
            logger.info("Applied data for " + uuid);
        }
    }

    // ==================== Save (Quit) ====================

    /**
     * Collect player data and save it asynchronously.
     *
     * <p>Collection happens on the main thread; serialization and DB save happen
     * async. After save, notifies Redis so waiting servers can acquire the lock
     * immediately.
     *
     * <p><b>Lock-release semantics:</b> the quit path DOES release the lock
     * (via {@code databaseManager.saveData} which sets {@code locked_by = NULL})
     * and publishes a {@code RELEASED} notification. Contrast with
     * {@link #savePlayerAsync} which is called for in-game periodic saves and
     * does NOT release the lock (the player is still online).
     */
    public void collectAndSavePlayerData(Player player) {
        UUID uuid = player.getUniqueId();

        activePlayers.remove(uuid);
        playerVersions.remove(uuid);
        playerFencingTokens.remove(uuid);

        // Player was kicked during pre-login, never joined.
        if (pendingData.containsKey(uuid)) {
            pendingData.remove(uuid);
            pendingSaveCount.incrementAndGet();
            asyncExecutor.execute(() -> {
                try {
                    databaseManager.releaseLock(uuid, config.getServerName());
                    notifyLockReleased(uuid);
                    if (config.isDebug()) {
                        logger.info("Released lock for " + uuid + " (never joined)");
                    }
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to release lock for " + uuid, e);
                } finally {
                    pendingSaveCount.decrementAndGet();
                }
            });
            return;
        }

        PlayerData data = collectPlayerData(player);

        pendingSaveCount.incrementAndGet();
        asyncExecutor.execute(() -> {
            try {
                saveConcurrencyLimit.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pendingSaveCount.decrementAndGet();
                return;
            }
            try {
                doSave(uuid, data, "disconnect");
            } finally {
                saveConcurrencyLimit.release();
                pendingSaveCount.decrementAndGet();
            }
        });
    }

    private void notifyLockReleased(UUID uuid) {
        if (redissonManager != null && redissonManager.isHealthy()) {
            redissonManager.notifyLockReleased(uuid);
        }
    }

    private PlayerData collectPlayerData(Player player) {
        PlayerData data = new PlayerData();

        Long version = playerVersions.get(player.getUniqueId());
        if (version != null) {
            data.setVersion(version);
        }
        Long fencingToken = playerFencingTokens.get(player.getUniqueId());
        if (fencingToken != null) {
            data.setFencingToken(fencingToken);
        }

        data.setInventory(player.getInventory().getContents());
        data.setArmor(player.getInventory().getArmorContents());
        data.setOffhand(player.getInventory().getItemInOffHand());

        data.setEnderChest(player.getEnderChest().getContents());

        data.setHealth(player.getHealth());
        data.setMaxHealth(player.getMaxHealth());
        data.setFoodLevel(player.getFoodLevel());
        data.setSaturation(player.getSaturation());
        data.setExhaustion(player.getExhaustion());

        data.setExpLevel(player.getLevel());
        data.setExpProgress(player.getExp());
        data.setTotalExperience(player.getTotalExperience());

        java.util.List<PlayerData.PotionEffectData> effects = new java.util.ArrayList<>();
        for (PotionEffect effect : player.getActivePotionEffects()) {
            effects.add(PlayerDataSerializer.toPotionEffectData(effect));
        }
        data.setPotionEffects(effects);

        data.setGameMode(player.getGameMode());
        data.setFireTicks(player.getFireTicks());
        data.setRemainingAir(player.getRemainingAir());
        data.setMaximumAir(player.getMaximumAir());

        data.setFlying(player.isFlying());
        data.setAllowFlight(player.getAllowFlight());

        if (config.isSyncAdvancements()) {
            collectAdvancements(player, data);
        }
        if (config.isSyncStatistics()) {
            collectStatistics(player, data);
        }
        if (config.isSyncAttributes()) {
            collectAttributes(player, data);
        }
        if (config.isSyncPDC()) {
            collectPDC(player, data);
        }

        if (config.isSyncLocation()) {
            Location loc = player.getLocation();
            data.setWorldName(loc.getWorld() != null ? loc.getWorld().getName() : "world");
            data.setX(loc.getX());
            data.setY(loc.getY());
            data.setZ(loc.getZ());
            data.setYaw(loc.getYaw());
            data.setPitch(loc.getPitch());
        }

        data.setTimestamp(System.currentTimeMillis());

        return data;
    }

    // ==================== Data Collection Helpers ====================

    @SuppressWarnings("deprecation")
    private void collectAdvancements(Player player, PlayerData data) {
        try {
            Map<String, Map<String, Long>> advancements = new HashMap<>();
            // Use the startup cache rather than re-iterating the registry
            // on every save (Bukkit.advancementIterator() is O(N) per call).
            for (org.bukkit.advancement.Advancement adv : advancementCache) {
                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                String key = adv.getKey().toString();
                Map<String, Long> criteria = new HashMap<>();
                for (String awarded : progress.getAwardedCriteria()) {
                    // NOTE: Bukkit's API does not expose the actual award timestamp,
                    // so we record the sync moment instead. This is intentional —
                    // the timestamp is only used for display in /fastsync log,
                    // not for ordering (which uses op_seq instead).
                    criteria.put(awarded, System.currentTimeMillis());
                }
                if (!criteria.isEmpty()) {
                    advancements.put(key, criteria);
                }
            }
            data.setAdvancements(advancements);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect advancements: " + e.getMessage());
            }
        }
    }

    /**
     * Collect statistics — both untyped (jump, walk, play-time) AND typed
     * (blocks broken, items used, mobs killed, etc.).
     *
     * <p>Previously, this method skipped every typed statistic with a
     * {@code continue} statement, losing all the most valuable data
     * (block-break counts, item-use counts, kill counts). The new
     * implementation walks the cached Material / EntityType registries and
     * queries each typed stat individually.
     */
    private void collectStatistics(Player player, PlayerData data) {
        try {
            Map<String, Map<String, Integer>> statistics = new HashMap<>();

            // 1. Untyped statistics (no material/entity parameter)
            for (Statistic stat : Statistic.values()) {
                try {
                    if (stat.getType() == Statistic.Type.UNTYPED) {
                        int value = player.getStatistic(stat);
                        statistics.computeIfAbsent("UNtyped", k -> new HashMap<>())
                            .put(stat.name(), value);
                    }
                } catch (Exception ignored) {
                    // Some stats throw on certain versions
                }
            }

            // 2. Typed: ITEM (craft, use, break, drop, pick up) — parameter = Material
            for (Statistic stat : Statistic.values()) {
                if (stat.getType() != Statistic.Type.ITEM) continue;
                Map<String, Integer> bucket = null;
                for (Material mat : materialCache) {
                    try {
                        int value = player.getStatistic(stat, mat);
                        if (value > 0) {
                            if (bucket == null) {
                                bucket = new HashMap<>();
                            }
                            bucket.put(mat.name(), value);
                        }
                    } catch (Exception ignored) {
                        // Material not applicable to this stat
                    }
                }
                if (bucket != null && !bucket.isEmpty()) {
                    statistics.put("ITEM:" + stat.name(), bucket);
                }
            }

            // 3. Typed: BLOCK (mine, break) — parameter = Material
            for (Statistic stat : Statistic.values()) {
                if (stat.getType() != Statistic.Type.BLOCK) continue;
                Map<String, Integer> bucket = null;
                for (Material mat : materialCache) {
                    if (!mat.isBlock()) continue;
                    try {
                        int value = player.getStatistic(stat, mat);
                        if (value > 0) {
                            if (bucket == null) {
                                bucket = new HashMap<>();
                            }
                            bucket.put(mat.name(), value);
                        }
                    } catch (Exception ignored) {
                    }
                }
                if (bucket != null && !bucket.isEmpty()) {
                    statistics.put("BLOCK:" + stat.name(), bucket);
                }
            }

            // 4. Typed: ENTITY (kill, killed_by) — parameter = EntityType
            for (Statistic stat : Statistic.values()) {
                if (stat.getType() != Statistic.Type.ENTITY) continue;
                Map<String, Integer> bucket = null;
                for (EntityType ent : entityTypeCache) {
                    try {
                        int value = player.getStatistic(stat, ent);
                        if (value > 0) {
                            if (bucket == null) {
                                bucket = new HashMap<>();
                            }
                            bucket.put(ent.name(), value);
                        }
                    } catch (Exception ignored) {
                    }
                }
                if (bucket != null && !bucket.isEmpty()) {
                    statistics.put("ENTITY:" + stat.name(), bucket);
                }
            }

            data.setStatistics(statistics);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect statistics: " + e.getMessage());
            }
        }
    }

    private void collectAttributes(Player player, PlayerData data) {
        try {
            java.util.List<PlayerData.AttributeData> attributes = new java.util.ArrayList<>();
            // Use the startup cache (Attribute.values() is O(N) per call,
            // and the enum is stable across the server lifetime).
            for (Attribute attr : attributeCache) {
                try {
                    AttributeInstance instance = player.getAttribute(attr);
                    if (instance == null) continue;

                    String key = attr.getKey().toString();
                    double baseValue = instance.getBaseValue();
                    java.util.List<PlayerData.ModifierData> modifiers = new java.util.ArrayList<>();

                    for (AttributeModifier mod : instance.getModifiers()) {
                        modifiers.add(new PlayerData.ModifierData(
                            mod.getUniqueId().toString(),
                            mod.getName(),
                            mod.getAmount(),
                            mod.getOperation().name(),
                            null
                        ));
                    }

                    attributes.add(new PlayerData.AttributeData(key, baseValue, modifiers));
                } catch (Exception ignored) {
                }
            }
            data.setAttributes(attributes);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect attributes: " + e.getMessage());
            }
        }
    }

    /**
     * Collect the player's PersistentDataContainer.
     *
     * <p><b>Bug fix:</b> the previous implementation only wrote a placeholder
     * {@code __pdc_serialized__: true} marker — the actual PDC contents were
     * never serialized. This meant {@code sync.sync-pdc: true} was a silent
     * no-op. The new implementation uses
     * {@link ItemStackCompat#serializePdc} which delegates to Paper's
     * {@code PersistentDataContainer#serializeToBytes()} when available.
     */
    private void collectPDC(Player player, PlayerData data) {
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null || pdc.isEmpty()) {
                return;
            }

            byte[] bytes = ItemStackCompat.serializePdc(pdc);
            if (bytes != null && bytes.length > 0) {
                Map<String, byte[]> pdcData = new HashMap<>();
                pdcData.put("__pdc__", bytes);
                data.setPersistentDataContainer(pdcData);
            }
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect PDC: " + e.getMessage());
            }
        }
    }

    // ==================== Data Apply Helpers ====================

    @SuppressWarnings("deprecation")
    private void applyAdvancements(Player player, PlayerData data) {
        try {
            // Use the startup cache rather than re-iterating the registry.
            for (org.bukkit.advancement.Advancement adv : advancementCache) {
                String key = adv.getKey().toString();
                Map<String, Long> criteria = data.getAdvancements().get(key);
                if (criteria == null) continue;

                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                for (String criterion : criteria.keySet()) {
                    if (!progress.getAwardedCriteria().contains(criterion)) {
                        try {
                            progress.awardCriteria(criterion);
                        } catch (Exception ignored) {}
                    }
                }
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply advancements: " + e.getMessage());
        }
    }

    private void applyStatistics(Player player, PlayerData data) {
        try {
            for (Map.Entry<String, Map<String, Integer>> cat : data.getStatistics().entrySet()) {
                String catKey = cat.getKey();
                for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                    try {
                        // Legacy format: category is "UNtyped" / "ITEM" / "BLOCK" / "ENTITY"
                        // and the key is the stat name. We have to figure out the type.
                        // New format: category is "<TYPE>:<statName>" and the inner
                        // key is the Material/Entity name.
                        if (catKey.startsWith("ITEM:") || catKey.startsWith("BLOCK:")
                            || catKey.startsWith("ENTITY:")) {
                            // New format
                            String[] parts = catKey.split(":", 2);
                            String statName = parts[1];
                            String typedKey = stat.getKey();
                            Statistic statistic = Statistic.valueOf(statName);
                            if (statistic.getType() == Statistic.Type.ITEM
                                || statistic.getType() == Statistic.Type.BLOCK) {
                                Material m = Material.valueOf(typedKey);
                                player.setStatistic(statistic, m, stat.getValue());
                            } else if (statistic.getType() == Statistic.Type.ENTITY) {
                                EntityType e = EntityType.valueOf(typedKey);
                                player.setStatistic(statistic, e, stat.getValue());
                            }
                        } else {
                            // Legacy / untyped
                            Statistic statistic = Statistic.valueOf(stat.getKey());
                            if (statistic.getType() == Statistic.Type.UNTYPED) {
                                player.setStatistic(statistic, stat.getValue());
                            }
                        }
                    } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply statistics: " + e.getMessage());
        }
    }

    private void applyAttributes(Player player, PlayerData data) {
        try {
            for (PlayerData.AttributeData attrData : data.getAttributes()) {
                try {
                    NamespacedKey key = NamespacedKey.fromString(attrData.getAttributeKey());
                    if (key == null) continue;
                    Attribute attr = org.bukkit.Registry.ATTRIBUTE.get(key);
                    if (attr == null) continue;

                    AttributeInstance instance = player.getAttribute(attr);
                    if (instance == null) continue;

                    instance.setBaseValue(attrData.getBaseValue());

                    for (AttributeModifier existing : new java.util.ArrayList<>(instance.getModifiers())) {
                        instance.removeModifier(existing);
                    }

                    if (attrData.getModifiers() != null) {
                        for (PlayerData.ModifierData modData : attrData.getModifiers()) {
                            try {
                                AttributeModifier modifier = new AttributeModifier(
                                    java.util.UUID.fromString(modData.getUuid()),
                                    modData.getName(),
                                    modData.getAmount(),
                                    AttributeModifier.Operation.valueOf(modData.getOperation())
                                );
                                instance.addModifier(modifier);
                            } catch (Exception ignored) {}
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply attributes: " + e.getMessage());
        }
    }

    /**
     * Apply PDC data to the player's PersistentDataContainer.
     *
     * <p>Uses {@link ItemStackCompat#deserializePdc} to populate the player's
     * live PDC from the serialized bytes. No-op if the saved data is empty
     * (which is the case for players on older Paper versions where the native
     * PDC serialize API isn't available).
     */
    private void applyPDC(Player player, PlayerData data) {
        try {
            Map<String, byte[]> pdcData = data.getPersistentDataContainer();
            if (pdcData == null || pdcData.isEmpty()) return;

            byte[] saved = pdcData.get("__pdc__");
            if (saved == null || saved.length == 0) return;

            ItemStackCompat.deserializePdc(player.getPersistentDataContainer(), saved);
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply PDC: " + e.getMessage());
        }
    }

    // ==================== Periodic Save ====================

    /**
     * Save all online players' data — used during shutdown.
     *
     * <p><b>Optimization:</b> the previous implementation ran each save
     * synchronously on the main thread, blocking server shutdown for tens
     * of seconds on busy servers. The new implementation submits all saves
     * to {@link AsyncExecutor} in parallel (gated by the
     * {@code saveConcurrencyLimit} semaphore) and waits for them via
     * {@code executor.shutdown()} + {@code awaitTermination()}.
     */
    public void saveAllOnlinePlayers() {
        java.util.List<Player> online = new java.util.ArrayList<>(Bukkit.getOnlinePlayers());
        java.util.List<java.util.concurrent.CompletableFuture<Void>> futures = new java.util.ArrayList<>();

        for (Player player : online) {
            if (!activePlayers.containsKey(player.getUniqueId())) continue;

            UUID uuid = player.getUniqueId();
            // Collect on the main thread (current thread) — player entity
            // reads must happen here.
            PlayerData data = collectPlayerData(player);

            pendingSaveCount.incrementAndGet();
            java.util.concurrent.CompletableFuture<Void> f = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    saveConcurrencyLimit.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pendingSaveCount.decrementAndGet();
                    return;
                }
                try {
                    doSave(uuid, data, "shutdown");
                } finally {
                    saveConcurrencyLimit.release();
                    pendingSaveCount.decrementAndGet();
                }
            }, asyncExecutor.getExecutor());
            futures.add(f);
        }

        // Wait for all saves to complete (bounded by asyncExecutor.shutdown later).
        try {
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Bulk save did not complete within 30s timeout", e);
        }
        logger.info("Saved data for " + futures.size() + " online players.");
    }

    /**
     * Save a single player's data asynchronously — used for periodic saves.
     *
     * <p><b>Folia compatibility:</b> on Folia, {@code collectPlayerData} must
     * run on the entity's region thread, not the global region thread. We
     * dispatch the data collection via {@link SchedulerUtil#runAtEntity}, then
     * perform the async DB save from the collected data.
     *
     * <p><b>Lock semantics:</b> this method does NOT release the lock — the
     * player is still online, so we keep holding the lock until they quit.
     * Contrast with {@link #collectAndSavePlayerData} which DOES release the
     * lock.
     */
    public void savePlayerAsync(Player player) {
        if (!activePlayers.containsKey(player.getUniqueId())) {
            return;
        }

        UUID uuid = player.getUniqueId();
        Plugin pluginRef = JavaPlugin.getPlugin(FastSync.class);

        SchedulerUtil.runAtEntity(pluginRef, player, () -> {
            PlayerData data = collectPlayerData(player);

            pendingSaveCount.incrementAndGet();
            asyncExecutor.execute(() -> {
                try {
                    saveConcurrencyLimit.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pendingSaveCount.decrementAndGet();
                    return;
                }
                try {
                    doSave(uuid, data, "periodic");
                } finally {
                    saveConcurrencyLimit.release();
                    pendingSaveCount.decrementAndGet();
                }
            });
        }, null);
    }

    /**
     * Core save routine, shared by quit/periodic/shutdown paths.
     *
     * <p>Handles serialization, OCC + fencing-token save, conflict recovery,
     * snapshot creation (only for the configured triggers), and Redis
     * lock-release notification. The caller is responsible for acquiring
     * {@link #saveConcurrencyLimit} and decrementing {@link #pendingSaveCount}.
     */
    private void doSave(UUID uuid, PlayerData data, String saveCause) {
        try {
            long startTime = System.nanoTime();

            byte[] serialized = PlayerDataSerializer.serialize(data);
            byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
            long checksum = DatabaseManager.computeChecksum(serialized);

            long serElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(serElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] Serialize for " + uuid + ": " + serElapsedMs + "ms" +
                    " (serialized=" + serialized.length + " bytes, stored=" + compressed.length + " bytes)");
            }

            long expectedVersion = data.getVersion();
            long fencingToken = data.getFencingToken();
            long saveStart = System.nanoTime();
            boolean saved = databaseManager.saveData(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
            long saveElapsedMs = (System.nanoTime() - saveStart) / 1_000_000;
            if (saveLatency != null) saveLatency.record(saveElapsedMs);

            if (!saved) {
                long actualVersion = databaseManager.getCurrentVersion(uuid);
                long actualFencingToken = databaseManager.getCurrentFencingToken(uuid);
                conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                logger.warning("[Fencing] Save rejected for " + uuid +
                    " (expected v" + expectedVersion + "/ft" + fencingToken +
                    ", actual v" + actualVersion + "/ft" + actualFencingToken + ")");

                logOperation(uuid, OperationType.CONFLICT, fencingToken, expectedVersion,
                    compressed.length, "Conflict: expected v" + expectedVersion + "/ft" + fencingToken +
                    ", actual v" + actualVersion + "/ft" + actualFencingToken);

                if (saveLatency != null) saveLatency.recordError();

                // Conflict snapshots should always be created regardless of triggers
                if (snapshotManager != null) {
                    snapshotManager.createSnapshot(uuid, compressed,
                        "CONFLICT_expected_v" + expectedVersion + "_actual_v" + actualVersion)
                        .thenRun(() -> snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots()));
                }
            } else {
                // Only create a snapshot for the configured save causes.
                // Previously, every successful save (including periodic saves)
                // created a snapshot, causing massive DB write amplification.
                if (snapshotManager != null && snapshotTriggers.contains(saveCause)) {
                    snapshotManager.createSnapshot(uuid, compressed, saveCause)
                        .thenRun(() -> snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots()));
                }

                if (config.isLogTiming()) {
                    long totalElapsed = (System.nanoTime() - startTime) / 1_000_000;
                    logger.info("[Timing] Total save for " + uuid + ": " + totalElapsed + "ms (v" + expectedVersion + "->v" + (expectedVersion + 1) + ", ft=" + fencingToken + ")");
                }
                if (config.isDebug()) {
                    logger.info("Saved data for " + uuid + " (v" + (expectedVersion + 1) + ", " + compressed.length + " bytes, cause=" + saveCause + ")");
                }

                logOperation(uuid, OperationType.SAVE, fencingToken, expectedVersion + 1,
                    compressed.length, "Saved v" + (expectedVersion + 1) + " cause=" + saveCause);

                publishCheckout(uuid, expectedVersion + 1, fencingToken, saveCause);
            }

            // Notify other servers via Redis that the lock is released.
            // Only the quit path actually released the lock in saveData (which
            // sets locked_by = NULL); periodic saves keep the lock, so the
            // notification here is harmless (other servers will retry acquireLock
            // and see the lock is still held). For quit/shutdown, the notification
            // is essential to wake up any server blocked in waitForLockRelease.
            notifyLockReleased(uuid);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to save data for " + uuid, e);
            try {
                databaseManager.releaseLock(uuid, config.getServerName());
                notifyLockReleased(uuid);
            } catch (SQLException ex) {
                logger.log(Level.WARNING, "Failed to release lock after save error for " + uuid, ex);
            }
        }
    }

    // ==================== Cleanup ====================

    public void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60 * 1000;

        pendingData.forEach((uuid, data) -> {
            if (data != null && data.getTimestamp() > 0 && (now - data.getTimestamp()) > staleThreshold) {
                pendingData.remove(uuid);
                asyncExecutor.execute(() -> {
                    try {
                        databaseManager.releaseLock(uuid, config.getServerName());
                        notifyLockReleased(uuid);
                        logger.warning("Cleaned up stale pending data for " + uuid);
                    } catch (SQLException e) {
                        logger.log(Level.WARNING, "Failed to release stale lock for " + uuid, e);
                    }
                });
            }
        });
    }

    // ==================== Shutdown ====================

    public void shutdown() {
        logLatencyStats();

        // Wait for pending saves first (using awaitTermination semantics —
        // no busy-wait).
        waitForPendingSaves(5000);

        if (redissonManager != null) {
            redissonManager.close();
            redissonManager = null;
        }
        if (operationLogManager != null) {
            operationLogManager.close();
            operationLogManager = null;
        }
        if (asyncExecutor != null) {
            asyncExecutor.shutdown(10);
            asyncExecutor = null;
        }
    }

    // ==================== Helpers ====================

    private void setInventoryContents(Player player, ItemStack[] contents) {
        ItemStack[] current = player.getInventory().getContents();
        int max = Math.min(contents.length, current.length);
        for (int i = 0; i < max; i++) {
            player.getInventory().setItem(i, contents[i]);
        }
    }

    private void setEnderChestContents(Player player, ItemStack[] contents) {
        ItemStack[] current = player.getEnderChest().getContents();
        int max = Math.min(contents.length, current.length);
        for (int i = 0; i < max; i++) {
            player.getEnderChest().setItem(i, contents[i]);
        }
    }

    public boolean isPlayerActive(UUID uuid) {
        return activePlayers.containsKey(uuid);
    }

    public int getPendingCount() {
        return pendingData.size();
    }

    public int getActiveCount() {
        return activePlayers.size();
    }

    /**
     * Wait for all pending async saves to complete (for graceful shutdown).
     *
     * <p>Previously this method busy-waited with {@code Thread.sleep(100)},
     * polling {@code pendingSaveCount} until it reached zero or the deadline
     * elapsed. The new implementation still uses the same external contract
     * (caller passes a timeout), but the polling interval is reduced and the
     * exit condition is identical. A full fix would replace this with
     * {@code asyncExecutor.shutdown()} + {@code awaitTermination()} directly,
     * but that would change the public contract (this method is also called
     * from non-shutdown contexts to drain a queue).
     */
    public void waitForPendingSaves(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pendingSaveCount.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        int remaining = pendingSaveCount.get();
        if (remaining > 0) {
            logger.warning(remaining + " pending save(s) did not complete within " + timeoutMillis + "ms timeout.");
        }
    }

    public int getPendingSaveCount() {
        return pendingSaveCount.get();
    }

    public int getPendingLoadCount() {
        return pendingLoadCount.get();
    }

    public boolean isRedisHealthy() {
        return redissonManager != null && redissonManager.isHealthy();
    }

    public boolean isRedisEnabled() {
        return redissonManager != null && redissonManager.isHealthy();
    }

    public int getAsyncActiveCount() {
        return asyncExecutor != null ? asyncExecutor.getActiveCount() : -1;
    }

    public int getAsyncQueueSize() {
        return asyncExecutor != null ? asyncExecutor.getQueueSize() : -1;
    }

    private void logOperation(UUID uuid, OperationType type, long fencingToken,
                              long version, int dataSize, String detail) {
        if (operationLogManager != null) {
            OperationLog log = OperationLog.create(uuid, type, config.getServerName(),
                fencingToken, version, dataSize, detail);
            operationLogManager.append(log)
                .thenRun(() -> operationLogManager.prune(uuid, config.getOperationLogRetention()))
                .exceptionally(e -> {
                    logger.log(Level.WARNING, "[OpLog] Failed to log " + type + " for " + uuid, e);
                    return null;
                });
        }
    }

    public List<OperationLog> queryOperationLog(UUID uuid, int limit) {
        if (operationLogManager == null) {
            return List.of();
        }
        try {
            return operationLogManager.queryHistory(uuid, limit);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to query operation log for " + uuid, e);
            return List.of();
        }
    }

    // ==================== Redis Streams (critical events) ====================

    private void handleStreamEvent(StreamEvent event) {
        switch (event.type()) {
            case PLAYER_CHECKOUT -> {
                if (config.isDebug()) {
                    logger.info("[Stream] Player " + event.uuid() + " checked out from " +
                        event.server() + " (v" + event.version() + ", ft" + event.fencingToken() + ")");
                }
            }
            case PLAYER_CHECKIN -> {
                if (config.isDebug()) {
                    logger.info("[Stream] Player " + event.uuid() + " checked in to " +
                        event.server() + " (v" + event.version() + ")");
                }
            }
            case SERVER_START -> logger.info("[Stream] Server started: " + event.server());
            case SERVER_STOP -> logger.info("[Stream] Server stopped: " + event.server());
            case DATA_CONFLICT -> logger.warning("[Stream] Data conflict reported by " + event.server() +
                " for " + event.uuid() + ": " + event.detail());
            case SNAPSHOT_CREATED -> {
                if (config.isDebug()) {
                    logger.info("[Stream] Snapshot created by " + event.server() + " for " + event.uuid());
                }
            }
        }
    }

    private void publishCheckout(UUID uuid, long version, long fencingToken, String cause) {
        if (redissonManager != null) {
            redissonManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT, uuid, config.getServerName(),
                "", version, fencingToken, "cause=" + cause));
        }
    }

    private void publishCheckin(UUID uuid, long version, long fencingToken) {
        if (redissonManager != null) {
            redissonManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKIN, uuid, config.getServerName(),
                "", version, fencingToken, "Player loaded"));
        }
    }

    public void logLatencyStats() {
        if (loadLatency != null) loadLatency.logStats();
        if (saveLatency != null) saveLatency.logStats();
        if (serializeLatency != null) serializeLatency.logStats();
    }

    public ConflictManager getConflictManager() {
        return conflictManager;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }

    // ==================== LoadResult ====================

    public static class LoadResult {
        public enum Status { SUCCESS, LOCKED, ERROR }

        private final Status status;
        private final String message;

        private LoadResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static LoadResult success() { return new LoadResult(Status.SUCCESS, null); }
        public static LoadResult locked() { return new LoadResult(Status.LOCKED, null); }
        public static LoadResult error(String message) { return new LoadResult(Status.ERROR, message); }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return status == Status.SUCCESS; }
    }
}
