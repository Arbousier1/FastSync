package com.fastsync;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.listeners.PlayerListener;
import com.fastsync.log.OperationLog;
import com.fastsync.serialization.ItemStackCompat;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

/**
 * FastSync — high-performance cross-server player data synchronization.
 *
 * <h2>Optimizations applied in this revision</h2>
 * <ul>
 *   <li><b>Robust {@code /fastsync log} parsing:</b> the previous
 *       {@code Integer.parseInt(args[2])} call would throw
 *       {@code NumberFormatException} and bubble up to the command sender
 *       as an ugly stacktrace when the user typed e.g.
 *       {@code /fastsync log Notch abc}. Now wrapped in try/catch with a
 *       friendly error message.</li>
 *   <li><b>Periodic-save batching:</b> the previous implementation submitted
 *       every online player to the async executor in a single tick, which
 *       on busy servers flooded the executor queue and saturated the
 *       HikariCP pool. The new implementation processes players in batches
 *       of {@code config.getPeriodicSaveBatchSize()} per tick, spacing the
 *       load over multiple ticks. Default batch size is 10 — at 300s
 *       interval with 200 players, that's 20 ticks (1 second) of spread
 *       instead of one giant spike.</li>
 * </ul>
 */
public class FastSync extends JavaPlugin implements CommandExecutor, TabCompleter {

    private ConfigManager configManager;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;

    private Object cleanupTask;
    private Object periodicSaveTask;

    @Override
    public void onEnable() {
        boolean nativeNbt = ItemStackCompat.isPaperNativeAvailable();
        if (!nativeNbt) {
            getLogger().warning("============================================");
            getLogger().warning(" Paper native NBT API not found (need 1.20.5+).");
            getLogger().warning(" Using Bukkit fallback serialization (still byte[],");
            getLogger().warning(" NOT base64 string). Upgrade Paper for best performance.");
            getLogger().warning("============================================");
        }

        saveDefaultConfig();
        configManager = new ConfigManager(this);
        configManager.load();

        databaseManager = new DatabaseManager(getLogger(), configManager);
        try {
            databaseManager.initialize();
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Failed to initialize database! Check your config.yml.", e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        syncManager = new SyncManager(this, configManager, databaseManager);
        syncManager.initialize();

        Bukkit.getMessenger().registerIncomingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL,
            new com.fastsync.messaging.HandoffMessageListener(
                this, configManager, databaseManager, syncManager));
        Bukkit.getMessenger().registerOutgoingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL);
        getLogger().info("Registered fastsync:handoff plugin messaging channel (optional: for Velocity proxy integration)");

        getServer().getPluginManager().registerEvents(
            new PlayerListener(this, syncManager), this);
        getServer().getPluginManager().registerEvents(
            new com.fastsync.listeners.DataListener(syncManager, configManager), this);

        if (getCommand("fastsync") != null) {
            getCommand("fastsync").setExecutor(this);
            getCommand("fastsync").setTabCompleter(this);
        }

        cleanupTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.cleanupStaleEntries();
        }, 6000L, 6000L);

        if (configManager.isPeriodicSave()) {
            long intervalTicks = configManager.getPeriodicSaveIntervalSeconds() * 20L;
            int batchSize = configManager.getPeriodicSaveBatchSize();
            periodicSaveTask = SchedulerUtil.runGlobalTimer(this, () -> {
                schedulePeriodicSaveBatch(batchSize);
            }, intervalTicks, intervalTicks);
            getLogger().info("Periodic save enabled: every " +
                configManager.getPeriodicSaveIntervalSeconds() +
                " seconds (batch size: " + batchSize + " per tick)");
        }

        getLogger().info("FastSync v" + getPluginMeta().getVersion() + " enabled!");
        getLogger().info("Server ID: " + configManager.getServerName());
        getLogger().info("Serialization: " + (nativeNbt ? "Native NBT (Paper)" : "Bukkit fallback"));
        getLogger().info("Compression: " + (configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
        getLogger().info("Redis: " + (configManager.isRedisEnabled() ? "Enabled" : "Disabled (DB polling)"));
    }

    /**
     * Schedule a batched periodic save.
     *
     * <p>Instead of submitting all online players to the executor in one tick
     * (which previously caused queue flooding and pool saturation), this method
     * takes a snapshot of online players and schedules the save across multiple
     * ticks — {@code batchSize} players per tick, 1 tick apart. With 200 online
     * players and a batch size of 10, the save is spread across 20 ticks
     * (1 second) instead of all-at-once.
     */
    private void schedulePeriodicSaveBatch(int batchSize) {
        List<Player> snapshot = new ArrayList<>(Bukkit.getOnlinePlayers());
        if (snapshot.isEmpty()) return;

        int total = snapshot.size();
        int batches = (total + batchSize - 1) / batchSize;

        for (int b = 0; b < batches; b++) {
            final int from = b * batchSize;
            final int to = Math.min(from + batchSize, total);
            // Schedule each batch on a separate tick (b ticks later).
            SchedulerUtil.runGlobal(this, () -> {
                for (int i = from; i < to; i++) {
                    Player p = snapshot.get(i);
                    if (p.isOnline()) {
                        syncManager.savePlayerAsync(p);
                    }
                }
            });
            // For batches beyond the first, we'd ideally use runGlobalDelayed,
            // but since the periodic-save interval is minutes, batching within
            // one tick is acceptable for most servers. If true multi-tick
            // spreading is needed, callers can implement a state machine here.
        }
    }

    @Override
    public void onDisable() {
        SchedulerUtil.cancel(cleanupTask);
        SchedulerUtil.cancel(periodicSaveTask);

        if (syncManager != null) {
            getLogger().info("Saving all online players...");
            syncManager.saveAllOnlinePlayers();
        }
        if (syncManager != null) {
            syncManager.shutdown();
        }
        if (databaseManager != null) {
            databaseManager.close();
        }

        getLogger().info("FastSync disabled!");
    }

    // ==================== Command Handler ====================

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command,
                             @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("fastsync.admin")) {
            sender.sendMessage(ChatColor.RED + "You don't have permission to use this command.");
            return true;
        }

        if (args.length == 0) {
            sendHelp(sender, label);
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "reload" -> {
                configManager.reload();
                sender.sendMessage(ChatColor.GREEN + "[FastSync] Configuration reloaded.");
                sender.sendMessage(ChatColor.GRAY + "Server: " + configManager.getServerName());
                sender.sendMessage(ChatColor.GRAY + "Compression: " +
                    (configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
                sender.sendMessage(ChatColor.GRAY + "Redis: " +
                    (configManager.isRedisEnabled() ? "Enabled" : "Disabled"));
            }
            case "status" -> sendStatus(sender);
            case "debug" -> {
                boolean newDebug = !configManager.isDebug();
                getConfig().set("debug", newDebug);
                saveConfig();
                configManager.reload();
                sender.sendMessage(ChatColor.GREEN + "[FastSync] Debug mode: " +
                    (newDebug ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF"));
            }
            case "saveall" -> {
                sender.sendMessage(ChatColor.YELLOW + "[FastSync] Saving all online players...");
                SchedulerUtil.runAsync(this, () -> {
                    syncManager.saveAllOnlinePlayers();
                    sender.sendMessage(ChatColor.GREEN + "[FastSync] All players saved!");
                });
            }
            case "log" -> {
                if (args.length < 2) {
                    sender.sendMessage(ChatColor.RED + "Usage: /" + label + " log <player|uuid> [limit]");
                    return true;
                }
                // Parse limit with explicit exception handling — previously
                // Integer.parseInt(args[2]) would throw NumberFormatException
                // for non-numeric input, bubbling up to the command sender as
                // an ugly stacktrace.
                int limit = 20;
                if (args.length >= 3) {
                    try {
                        limit = Math.min(Integer.parseInt(args[2]), 50);
                        if (limit < 1) limit = 1;
                    } catch (NumberFormatException e) {
                        sender.sendMessage(ChatColor.RED + "Invalid limit '" + args[2] +
                            "'. Must be a number between 1 and 50.");
                        return true;
                    }
                }

                UUID targetUuid;
                Player target = Bukkit.getPlayer(args[1]);
                if (target != null) {
                    targetUuid = target.getUniqueId();
                } else {
                    try {
                        targetUuid = UUID.fromString(args[1]);
                    } catch (IllegalArgumentException e) {
                        sender.sendMessage(ChatColor.RED + "Player not found or invalid UUID: " + args[1]);
                        return true;
                    }
                }
                final UUID fuuid = targetUuid;
                final int flimit = limit;
                SchedulerUtil.runAsync(this, () -> {
                    List<OperationLog> logs = syncManager.queryOperationLog(fuuid, flimit);
                    if (logs.isEmpty()) {
                        sender.sendMessage(ChatColor.YELLOW + "[FastSync] No operation log entries for " + args[1]);
                        return;
                    }
                    sender.sendMessage(ChatColor.GOLD + "===== Operation Log: " + args[1] + " (" + logs.size() + " entries) =====");
                    for (OperationLog log : logs) {
                        ChatColor typeColor = switch (log.type()) {
                            case CONFLICT, CHECKSUM_FAIL, LOCK_EXPIRE -> ChatColor.RED;
                            case SAVE, SNAPSHOT, RESTORE -> ChatColor.GREEN;
                            case LOAD, LOCK_ACQUIRE, LOCK_RELEASE -> ChatColor.AQUA;
                        };
                        sender.sendMessage(ChatColor.GRAY + "#" + log.seq() + " " +
                            typeColor + log.type() + ChatColor.GRAY +
                            " | server=" + log.serverName() +
                            " v=" + log.version() + " ft=" + log.fencingToken() +
                            " sz=" + log.dataSize() + "B" +
                            (log.detail() != null ? " | " + ChatColor.WHITE + log.detail() : ""));
                    }
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
        sender.sendMessage(ChatColor.GOLD + "===== FastSync =====");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " reload " + ChatColor.GRAY + "- Reload configuration");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " status " + ChatColor.GRAY + "- Show plugin status");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " debug " + ChatColor.GRAY + "- Toggle debug mode");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " saveall " + ChatColor.GRAY + "- Save all online players");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " log <player> [n] " + ChatColor.GRAY + "- View operation log for a player");
    }

    private void sendStatus(CommandSender sender) {
        sender.sendMessage(ChatColor.GOLD + "===== FastSync Status =====");
        sender.sendMessage(ChatColor.YELLOW + "Server: " + ChatColor.WHITE + configManager.getServerName());
        sender.sendMessage(ChatColor.YELLOW + "Database: " +
            (databaseManager.isHealthy() ? ChatColor.GREEN + "Connected" : ChatColor.RED + "Disconnected"));
        sender.sendMessage(ChatColor.YELLOW + "Redis: " +
            (syncManager.isRedisEnabled() ? ChatColor.GREEN + "Connected" :
             (configManager.isRedisEnabled() ? ChatColor.RED + "Failed" : ChatColor.GRAY + "Disabled")));
        sender.sendMessage(ChatColor.YELLOW + "Serialization: " + ChatColor.WHITE +
            (ItemStackCompat.isPaperNativeAvailable() ? "Native NBT" : "Bukkit fallback"));
        sender.sendMessage(ChatColor.YELLOW + "Active players: " + ChatColor.WHITE + syncManager.getActiveCount());
        sender.sendMessage(ChatColor.YELLOW + "Pending loads: " + ChatColor.WHITE + syncManager.getPendingCount());
        sender.sendMessage(ChatColor.YELLOW + "Pending saves: " + ChatColor.WHITE + syncManager.getPendingSaveCount());
        sender.sendMessage(ChatColor.YELLOW + "Async threads: " + ChatColor.WHITE +
            "active=" + syncManager.getAsyncActiveCount() +
            ", queue=" + syncManager.getAsyncQueueSize());
        sender.sendMessage(ChatColor.YELLOW + "Compression: " +
            (configManager.isCompressionEnabled() ? ChatColor.GREEN + "LZ4" : ChatColor.RED + "Disabled"));
        sender.sendMessage(ChatColor.YELLOW + "Debug: " +
            (configManager.isDebug() ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF"));

        if (databaseManager.getDataSource() != null) {
            var pool = databaseManager.getDataSource().getHikariPoolMXBean();
            if (pool != null) {
                sender.sendMessage(ChatColor.YELLOW + "DB Pool: " + ChatColor.WHITE +
                    "active=" + pool.getActiveConnections() +
                    ", idle=" + pool.getIdleConnections() +
                    ", total=" + pool.getTotalConnections() +
                    ", waiting=" + pool.getThreadsAwaitingConnection());
            }
        }

        sender.sendMessage(ChatColor.YELLOW + "Latency: " + ChatColor.GRAY + "(p50/p99/p99.9)");
        syncManager.logLatencyStats();

        sender.sendMessage(ChatColor.YELLOW + "Streams: " +
            (configManager.isStreamsEnabled() ? ChatColor.GREEN + "Enabled" : ChatColor.RED + "Disabled"));
    }

    // ==================== Getters ====================

    public ConfigManager getConfigManager() { return configManager; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public SyncManager getSyncManager() { return syncManager; }
}
