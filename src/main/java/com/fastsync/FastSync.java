package com.fastsync;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.i18n.MessageManager;
import com.fastsync.listeners.PlayerListener;
import com.fastsync.log.OperationLog;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

/**
 * FastSync - High-performance cross-server player data synchronization.
 *
 * Design principles (based on community discussion):
 *   1. NBT byte[] serialization - NO base64 string encoding, NO Kryo, NO Gson
 *      ItemStack.serializeAsBytes() (Paper 1.21.11+ native API; no fallback)
 *   2. LZ4 compression to reduce database storage and network transfer
 *   3. Data loaded during login phase (AsyncPlayerPreLoginEvent) - not after joining
 *      Prevents item duplication bugs from "enter server then load" approach
 *   4. Cross-server lock with proper acknowledgment (Redis pub/sub)
 *      NOT HuskSync's broken "petition" that forces entry after timeout
 *   5. Dedicated thread pool for async operations (NOT ForkJoinPool.commonPool)
 *   6. Version byte prefix for future serialization format migration
 */
public class FastSync extends JavaPlugin implements CommandExecutor, TabCompleter, Listener {

    private static String formatTime(long epochMillis) {
        return epochMillis > 0 ? new java.util.Date(epochMillis).toString() : "never";
    }

    private ConfigManager configManager;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;
    private MessageManager msg;

    private static FastSync instance;

    public static FastSync getInstance() { return instance; }
    public MessageManager getMessageManager() { return msg; }

    private Object cleanupTask;
    private Object periodicSaveTask;
    private Object heartbeatTask;
    private final java.util.concurrent.atomic.AtomicBoolean shutdownSavePrepared =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * Start (or restart) the heartbeat task. Called from onEnable and from
     * the reload command when heartbeat-interval-seconds changes.
     *
     * <p>Without this, /fastsync reload would change the config value but
     * the old timer would keep running at the old interval — a subtle
     * production trap that could cause lock expiry if the new interval is
     * shorter than expected.
     */
    private void restartHeartbeatTask() {
        // Cancel old task if running
        if (heartbeatTask != null) {
            SchedulerUtil.cancel(heartbeatTask);
            heartbeatTask = null;
        }
        // Start new task with current config
        long heartbeatTicks = configManager.getHeartbeatIntervalSeconds() * 20L;
        heartbeatTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.heartbeatOnlinePlayers();
        }, heartbeatTicks, heartbeatTicks);
        getLogger().info(msg.console("console.startup.heartbeat-restarted",
            configManager.getHeartbeatIntervalSeconds(), configManager.getLockTimeout()));
    }

    @Override
    public void onEnable() {
        instance = this;
        // Initialize config
        saveDefaultConfig();
        configManager = new ConfigManager(this);
        try {
            configManager.load();
        } catch (RuntimeException e) {
            // Clean-slate: config parse failure is a hard error (no Bukkit
            // fallback). Fail startup explicitly rather than letting the
            // RuntimeException propagate into Bukkit's loader.
            getLogger().log(Level.SEVERE, "Failed to load config.yml, refusing to start: " + e.getMessage(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize i18n message manager
        msg = new MessageManager(this, getLogger(), configManager.getLanguage());

        // Validate insecure default configuration before touching the DB.
        // Clean-slate: the bundled config ships sample (root/password,
        // sslMode=DISABLED) values; running with those in production is a
        // footgun. Fail startup unless the operator has explicitly opted in.
        try {
            configManager.validateProductionSafety();
        } catch (RuntimeException e) {
            getLogger().log(Level.SEVERE, "Refusing to start: " + e.getMessage(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize database
        databaseManager = new DatabaseManager(getLogger(), configManager);
        try {
            databaseManager.initialize();
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, msg.console("console.config.db-init-failed"), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize sync manager (creates thread pool + optional Redis)
        syncManager = new SyncManager(this, configManager, databaseManager);
        syncManager.initialize();

        // Register plugin messaging channel for proxy handoff communication
        // This is optional — if no Velocity proxy with FastSync Proxy is installed,
        // the channel simply never receives messages. The backend works standalone.
        Bukkit.getMessenger().registerIncomingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL,
            new com.fastsync.messaging.HandoffMessageListener(
                this, configManager, databaseManager, syncManager));
        Bukkit.getMessenger().registerOutgoingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL);
        getLogger().info(msg.console("console.startup.handoff-channel"));

        // Register listeners
        getServer().getPluginManager().registerEvents(this, this);
        getServer().getPluginManager().registerEvents(
            new PlayerListener(this, syncManager), this);
        getServer().getPluginManager().registerEvents(
            new com.fastsync.listeners.DataListener(syncManager, configManager), this);

        // Register dirty-tracking listener (component-level change detection)
        if (configManager.isDirtyTrackingEnabled() && syncManager.getDirtyMask() != null) {
            getServer().getPluginManager().registerEvents(
                new com.fastsync.listeners.dirty.DirtyTrackingListener(syncManager.getDirtyMask()), this);
            getLogger().info(msg.console("console.startup.dirty-tracking",
                configManager.getDirtyValidationInterval()));
        }

        // Register command
        if (getCommand("fastsync") != null) {
            getCommand("fastsync").setExecutor(this);
            getCommand("fastsync").setTabCompleter(this);
        }

        // Start cleanup task (every 5 minutes = 6000 ticks)
        cleanupTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.cleanupStaleEntries();
        }, 6000L, 6000L);

        // Start heartbeat task — refreshes locked_at for all online players.
        // This is the PRIMARY mechanism for keeping online locks alive.
        // Runs on async thread (DB I/O only, no Bukkit API calls).
        restartHeartbeatTask();

        // Start periodic save task (if enabled)
        if (configManager.isPeriodicSave()) {
            long intervalTicks = configManager.getPeriodicSaveIntervalSeconds() * 20L;
            periodicSaveTask = SchedulerUtil.runGlobalTimer(this, () -> {
                // Snapshot online players on the global region thread, then save them
                // in small batches spread across successive ticks to avoid a lag spike
                // when many players are online (process at most 10 players per tick).
                //
                // CRITICAL (Folia-safety): the batched dispatch MUST run on the
                // global region (runGlobalDelayed), NOT on an async thread
                // (runAsyncDelayed). savePlayerAsync(player) reads
                // player.getUniqueId() and calls SchedulerUtil.runAtEntity(plugin,
                // player, ...) — both touch the Player object. On Folia, async
                // threads must not touch Player entities; the global region is
                // the safe context for these reads. The actual DB write still
                // happens on the async executor (dispatched from inside
                // runAtEntity), so the global region is only used for the
                // brief per-player dispatch, not for the DB wait.
                List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
                final int batchSize = configManager.getPeriodicSaveBatchSize();
                for (int i = 0; i < players.size(); i += batchSize) {
                    final int start = i;
                    final int end = Math.min(i + batchSize, players.size());
                    long delayTicks = i / batchSize;
                    SchedulerUtil.runGlobalDelayed(this, () -> {
                        for (int j = start; j < end; j++) {
                            syncManager.savePlayerAsync(players.get(j));
                        }
                    }, delayTicks);
                }
            }, intervalTicks, intervalTicks);
            getLogger().info(msg.console("console.startup.periodic-save",
                configManager.getPeriodicSaveIntervalSeconds()));
        }

        getLogger().info(msg.console("console.startup.enabled", getPluginMeta().getVersion()));
        getLogger().info(msg.console("console.startup.server-id", configManager.getServerName()));
        getLogger().info(msg.console("console.startup.serialization"));
        getLogger().info(msg.console("console.startup.compression",
            configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
        getLogger().info(msg.console("console.startup.redis",
            configManager.isRedisEnabled() ? "Enabled" : "Disabled (DB polling)"));
    }

    @Override
    public void onDisable() {
        // The normal path already ran from PluginDisableEvent while scheduler
        // submissions were still legal. This remains as an idempotent fallback
        // for direct lifecycle invocations and standard Paper.
        prepareShutdownSave();

        // Shut down sync manager (waits for pending saves, closes Redis + thread pool)
        if (syncManager != null) {
            syncManager.shutdown();
        }

        // Close database
        if (databaseManager != null) {
            databaseManager.close();
        }

        getLogger().info(msg.console("console.shutdown.disabled"));
        instance = null;
    }

    /**
     * Paper fires PluginDisableEvent immediately before JavaPlugin is marked
     * disabled and onDisable() is invoked. This is the final point where Folia
     * entity-scheduler tasks may legally be submitted for final-state capture.
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onPluginDisable(PluginDisableEvent event) {
        if (event.getPlugin() == this) {
            prepareShutdownSave();
        }
    }

    private void prepareShutdownSave() {
        if (!shutdownSavePrepared.compareAndSet(false, true) || syncManager == null) {
            return;
        }

        // Close the online-save gate first so no periodic/death/world-save
        // snapshot can commit after this final save.
        syncManager.beginShutdown();
        SchedulerUtil.cancel(cleanupTask);
        SchedulerUtil.cancel(periodicSaveTask);
        SchedulerUtil.cancel(heartbeatTask);

        getLogger().info(msg.console("console.shutdown.saving"));
        SyncManager.SaveAllResult result =
            syncManager.saveAllOnlinePlayers(SyncManager.SaveKind.SHUTDOWN);
        getLogger().info(msg.console("console.shutdown.save-result",
            result.success(), result.total(),
            result.failed() > 0 ? ", " + result.failed() + " failed" : ""));
    }

    // ==================== Command Handler ====================

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command,
                             @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("fastsync.admin")) {
            sender.sendMessage(msg.component("command.no-permission"));
            return true;
        }

        if (args.length == 0) {
            sendHelp(sender, label);
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "reload" -> {
                configManager.reload();
                msg.reload(this);
                // Refresh SyncManager caches that depend on config (e.g. snapshot trigger set)
                if (syncManager != null) {
                    syncManager.refreshConfigCache();
                }
                // Restart heartbeat task — interval may have changed
                restartHeartbeatTask();
                sender.sendMessage(msg.component("command.reload.success"));
                sender.sendMessage(msg.component("command.reload.server", configManager.getServerName()));
                sender.sendMessage(msg.component("command.reload.compression",
                    configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
                sender.sendMessage(msg.component("command.reload.redis",
                    configManager.isRedisEnabled() ? "Enabled" : "Disabled"));
                sender.sendMessage(msg.component("command.reload.heartbeat",
                    configManager.getHeartbeatIntervalSeconds()));
                // The health probe borrows a JDBC connection and may wait up to
                // connection-timeout. Never perform it on a Paper/Folia tick.
                SchedulerUtil.runAsync(this, () -> {
                    boolean resetOk = syncManager.resetProtectionMode();
                    runForSender(sender, () -> sender.sendMessage(
                        resetOk
                            ? msg.component("command.reload.protection-reset")
                            : msg.component("command.reload.protection-active")));
                });
            }
            case "status" -> {
                sender.sendMessage(msg.component("command.status.checking"));
                SchedulerUtil.runAsync(this, () -> {
                    boolean databaseHealthy = databaseManager.isHealthy();
                    boolean redisHealthy = syncManager.isRedisHealthy();
                    runForSender(sender,
                        () -> sendStatus(sender, databaseHealthy, redisHealthy));
                });
            }
            case "debug" -> {
                boolean newDebug = !configManager.isDebug();
                getConfig().set("debug", newDebug);
                saveConfig();
                configManager.reload();
                sender.sendMessage(msg.component("command.debug.toggled",
                    newDebug ? "<green>ON" : "<red>OFF"));
            }
            case "saveall" -> {
                sender.sendMessage(msg.component("command.saveall.saving"));
                // CRITICAL (Folia-safety): the save flow is split into two phases:
                //
                //   Phase 1 (dispatch) — runs on the global region. Captures the
                //   player list via Bukkit.getOnlinePlayers(), then for each
                //   player reads player.getUniqueId() and dispatches via
                //   SchedulerUtil.runAtEntity. Both operations touch the Player
                //   object, which is forbidden from async threads on Folia.
                //
                //   Phase 2 (wait) — runs on the async executor. Iterates the
                //   futures returned by phase 1 and blocks on future.get() with
                //   a deadline. This phase does NOT touch any Player object, so
                //   it is safe to run on async. Moving it off the global region
                //   prevents /saveall from blocking global ticks while DB writes
                //   complete (up to 30s on large servers).
                SchedulerUtil.runGlobal(this, () -> {
                    List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
                    List<Map.Entry<UUID, java.util.concurrent.CompletableFuture<SyncManager.SaveResult>>> futures =
                        syncManager.dispatchPlayerSaves(players, SyncManager.SaveKind.BULK);
                    SchedulerUtil.runAsync(this, () -> {
                        try {
                            SyncManager.SaveAllResult result = syncManager.waitForPlayerSaves(futures, SyncManager.SaveKind.BULK);
                            SchedulerUtil.runGlobal(this, () -> {
                                if (result.allSucceeded()) {
                                    sender.sendMessage(msg.component("command.saveall.all-saved", result.total()));
                                } else {
                                    sender.sendMessage(msg.component("command.saveall.partial",
                                        result.success(), result.total(), result.failed()));
                                    if (!result.failures().isEmpty()) {
                                        sender.sendMessage(msg.component("command.saveall.failed-players"));
                                        result.failures().forEach((uuid, reason) ->
                                            sender.sendMessage(msg.component("command.saveall.failed-player", uuid, reason)));
                                    }
                                }
                            });
                        } catch (Exception e) {
                            getLogger().log(Level.SEVERE, msg.console("console.saveall.failed"), e);
                            SchedulerUtil.runGlobal(this, () ->
                                sender.sendMessage(msg.component("command.saveall.error", e.getMessage()))
                            );
                        }
                    });
                });
            }
            case "log" -> {
                if (args.length < 2) {
                    sender.sendMessage(msg.component("command.log.usage", label));
                    return true;
                }
                int limit;
                if (args.length >= 3) {
                    try {
                        limit = Math.min(Integer.parseInt(args[2]), 50);
                    } catch (NumberFormatException e) {
                        sender.sendMessage(msg.component("command.log.invalid-number", args[2]));
                        return true;
                    }
                } else {
                    limit = 20;
                }
                UUID targetUuid;
                Player target = Bukkit.getPlayer(args[1]);
                if (target != null) {
                    targetUuid = target.getUniqueId();
                } else {
                    try {
                        targetUuid = UUID.fromString(args[1]);
                    } catch (IllegalArgumentException e) {
                        sender.sendMessage(msg.component("command.log.player-not-found", args[1]));
                        return true;
                    }
                }
                final UUID fuuid = targetUuid;
                SchedulerUtil.runAsync(this, () -> {
                    List<OperationLog> logs = syncManager.queryOperationLog(fuuid, limit);
                    // Build all messages on async thread, then send on global thread
                    List<net.kyori.adventure.text.Component> components = new java.util.ArrayList<>();
                    if (logs.isEmpty()) {
                        components.add(msg.component("command.log.no-entries", args[1]));
                    } else {
                        components.add(msg.component("command.log.header", args[1], logs.size()));
                        for (OperationLog log : logs) {
                            String typeColor = switch (log.type()) {
                                case CONFLICT, CHECKSUM_FAIL, LOCK_EXPIRE -> "<red>";
                                case SAVE, SNAPSHOT, RESTORE -> "<green>";
                                case LOAD, LOCK_ACQUIRE, LOCK_RELEASE -> "<aqua>";
                            };
                            String detail = log.detail() != null ? " | <white>" + log.detail() : "";
                            components.add(msg.component("command.log.entry",
                                log.seq(), typeColor + log.type(), log.serverName(),
                                log.version(), log.fencingToken(), log.dataSize(), detail));
                        }
                    }
                    SchedulerUtil.runGlobal(this, () -> {
                        for (net.kyori.adventure.text.Component c : components) {
                            sender.sendMessage(c);
                        }
                    });
                });
            }
            default -> sendHelp(sender, label);
        }
        return true;
    }

    @Override
    public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command,
                                      @NotNull String alias, @NotNull String[] args) {
        if (args.length == 1) {
            List<String> suggestions = Arrays.asList("reload", "status", "debug", "saveall", "log");
            List<String> result = new ArrayList<>();
            for (String s : suggestions) {
                if (s.startsWith(args[0].toLowerCase())) {
                    result.add(s);
                }
            }
            return result;
        }
        return new ArrayList<>();
    }

    private void sendHelp(CommandSender sender, String label) {
        sender.sendMessage(msg.component("command.help.header"));
        sender.sendMessage(msg.component("command.help.reload", label));
        sender.sendMessage(msg.component("command.help.status", label));
        sender.sendMessage(msg.component("command.help.debug", label));
        sender.sendMessage(msg.component("command.help.saveall", label));
        sender.sendMessage(msg.component("command.help.log", label));
    }

    private void runForSender(CommandSender sender, Runnable task) {
        if (sender instanceof Player player) {
            SchedulerUtil.runAtEntity(this, player, task,
                () -> getLogger().fine(msg.console("console.sender-retired")));
        } else {
            SchedulerUtil.runGlobal(this, task);
        }
    }

    private void sendStatus(CommandSender sender, boolean databaseHealthy, boolean redisHealthy) {
        sender.sendMessage(msg.component("command.status.header"));
        sender.sendMessage(msg.component("command.status.server", configManager.getServerName()));
        sender.sendMessage(msg.component(databaseHealthy
            ? "command.status.database-connected" : "command.status.database-disconnected"));
        sender.sendMessage(msg.component(redisHealthy ? "command.status.redis-connected"
            : (configManager.isRedisEnabled() ? "command.status.redis-failed" : "command.status.redis-disabled")));
        sender.sendMessage(msg.component("command.status.serialization"));
        sender.sendMessage(msg.component("command.status.active-players", syncManager.getActiveCount()));
        sender.sendMessage(msg.component("command.status.pending-loads", syncManager.getPendingCount()));
        sender.sendMessage(msg.component("command.status.pending-saves", syncManager.getPendingSaveCount()));
        sender.sendMessage(msg.component("command.status.quarantined", syncManager.getQuarantinedPlayerCount()));
        sender.sendMessage(msg.component(syncManager.isProtectionMode()
            ? "command.status.protection-active" : "command.status.protection-off"));
        sender.sendMessage(msg.component("command.status.heartbeat",
            configManager.getHeartbeatIntervalSeconds(), configManager.getLockTimeout()));
        sender.sendMessage(msg.component("command.status.async-threads",
            syncManager.getAsyncActiveCount(), syncManager.getAsyncQueueSize()));
        sender.sendMessage(msg.component("command.status.login-load-budget",
            syncManager.getLoginLoadAvailablePermits(), syncManager.getLoginLoadLimit()));
        String finalSaveColor = syncManager.hasFinalSaveAlert() ? "<red>"
            : (syncManager.hasFinalSaveWarning() ? "<yellow>" : "<green>");
        sender.sendMessage(msg.component("command.status.final-save-executor",
            finalSaveColor,
            syncManager.getFinalSaveActiveCount(),
            syncManager.getFinalSaveQueueSize(), syncManager.getFinalSaveQueueCapacity(),
            syncManager.getFinalSaveQueueFullTotal(),
            formatTime(syncManager.getFinalSaveLastQueueFullAt())));
        sender.sendMessage(msg.component("command.status.final-save-spool-events",
            syncManager.getFinalSaveSpoolEnqueuedTotal(),
            formatTime(syncManager.getFinalSaveLastSpoolEnqueuedAt()),
            syncManager.getFinalSaveSpoolRejectedTotal(),
            formatTime(syncManager.getFinalSaveLastSpoolRejectedAt())));
        sender.sendMessage(msg.component("command.status.final-save-sync-fallback",
            syncManager.getFinalSaveSyncFallbackTotal(),
            formatTime(syncManager.getFinalSaveLastSyncFallbackAt())));
        // Spool disk state
        long spoolPending = syncManager.getFinalSaveSpoolPendingCount();
        long spoolFailed = syncManager.getFinalSaveSpoolFailedCount();
        if (spoolPending > 0 || spoolFailed > 0 || configManager.isFinalSaveSpoolEnabled()) {
            String spoolColor = spoolFailed > 0 ? "<red>" : (spoolPending > 0 ? "<yellow>" : "<green>");
            sender.sendMessage(msg.component("command.status.final-save-spool-disk",
                spoolColor, spoolPending, spoolFailed, syncManager.getFinalSaveSpoolBytes()));
            long lastReplay = syncManager.getFinalSaveSpoolLastReplayAt();
            if (lastReplay > 0) {
                sender.sendMessage(msg.component("command.status.final-save-spool-last-replay",
                    new java.util.Date(lastReplay)));
            }
            String lastError = syncManager.getFinalSaveSpoolLastError();
            if (lastError != null && !lastError.isEmpty()) {
                sender.sendMessage(msg.component("command.status.final-save-spool-last-error", lastError));
            }
            if (spoolFailed > 0) {
                sender.sendMessage(msg.component("command.status.final-save-spool-failed-entries",
                    configManager.getFinalSaveSpoolDir()));
            }
        }
        // Alerts
        if (syncManager.getFinalSaveSpoolRejectedTotal() > 0) {
            sender.sendMessage(msg.component("command.status.final-save-spool-rejected-alert"));
        }
        if (syncManager.getFinalSaveSyncFallbackTotal() > 0) {
            sender.sendMessage(msg.component("command.status.final-save-sync-fallback-alert"));
        }
        if (syncManager.hasFinalSaveWarning() && !syncManager.hasFinalSaveAlert()) {
            sender.sendMessage(msg.component("command.status.final-save-warning"));
        }
        if (configManager.isOperationLogEnabled()) {
            long dropped = syncManager.getOperationLogDroppedTotal();
            String opLogColor = dropped > 0 ? "<red>" : "<green>";
            sender.sendMessage(msg.component("command.status.oplog",
                opLogColor,
                syncManager.getOperationLogQueueSize(), syncManager.getOperationLogQueueCapacity(),
                dropped));
            if (dropped > 0) {
                long lastDropAt = syncManager.getOperationLogLastDropAt();
                sender.sendMessage(msg.component("command.status.oplog-alert",
                    lastDropAt > 0 ? " since " + new java.util.Date(lastDropAt) : ""));
            }
        } else {
            sender.sendMessage(msg.component("command.status.oplog-disabled"));
        }
        sender.sendMessage(msg.component(configManager.isCompressionEnabled()
            ? "command.status.compression-enabled" : "command.status.compression-disabled"));
        sender.sendMessage(msg.component(configManager.isDebug()
            ? "command.status.debug-on" : "command.status.debug-off"));

        // HikariCP stats
        if (databaseManager.getDataSource() != null) {
            var pool = databaseManager.getDataSource().getHikariPoolMXBean();
            if (pool != null) {
                sender.sendMessage(msg.component("command.status.db-pool",
                    pool.getActiveConnections(), pool.getIdleConnections(),
                    pool.getTotalConnections(), pool.getThreadsAwaitingConnection()));
            }
        }

        // Latency stats (Dynamo p99.9) — now sent to sender, not just logged
        sender.sendMessage(msg.component("command.status.latency-header"));
        for (String line : syncManager.getLatencyStatusLines()) {
            sender.sendMessage(msg.component("command.status.latency-line", line));
        }

        // Stream stats
        sender.sendMessage(msg.component(configManager.isStreamsEnabled()
            ? "command.status.streams-enabled" : "command.status.streams-disabled"));

        // Snapshot stats (round 14b)
        if (syncManager.getSnapshotManager() != null) {
            var sm = syncManager.getSnapshotManager();
            long rejected = sm.getRejectedCount();
            String snapColor = rejected > 0 ? "<red>" : "<green>";
            sender.sendMessage(msg.component("command.status.snapshots",
                snapColor, sm.getQueueSize(), sm.getQueueCapacity(), rejected));
        }

        // Cluster + production mode (round 14b)
        String clusterId = configManager.getClusterId();
        if (clusterId != null && !clusterId.isBlank()) {
            sender.sendMessage(msg.component("command.status.cluster", clusterId));
        } else {
            sender.sendMessage(msg.component("command.status.cluster-none"));
        }
        if (configManager.isProductionEnabled()) {
            sender.sendMessage(msg.component("command.status.production",
                configManager.isProductionRequireRedis() ? "required" : "optional",
                configManager.isFinalSaveAllowSyncFallback() ? "allowed" : "blocked"));
        }
    }

    // ==================== Getters ====================

    public ConfigManager getConfigManager() { return configManager; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public SyncManager getSyncManager() { return syncManager; }
}
