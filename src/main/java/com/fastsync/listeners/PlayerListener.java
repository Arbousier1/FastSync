package com.fastsync.listeners;

import com.fastsync.FastSync;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.ChatColor;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.UUID;

/**
 * Player event listener for synchronization.
 *
 * Key design: data is loaded during AsyncPlayerPreLoginEvent (before the player
 * actually joins), preventing the item duplication bugs that occur when data
 * is loaded after the player has already entered the server.
 */
public class PlayerListener implements Listener {

    private final FastSync plugin;
    private final SyncManager syncManager;

    public PlayerListener(FastSync plugin, SyncManager syncManager) {
        this.plugin = plugin;
        this.syncManager = syncManager;
    }

    /**
     * Load player data during the async pre-login phase.
     * This blocks the login until data is loaded, ensuring data is ready
     * before the player enters the server.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        // Skip if already disallowed by another plugin
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return;
        }

        // Skip for players with bypass permission (when configured)
        // Note: permissions aren't available at pre-login, so we can't check here

        UUID uuid = event.getUniqueId();

        SyncManager.LoadResult result = syncManager.loadPlayerData(uuid);

        if (!result.isSuccess()) {
            if (result.getStatus() == SyncManager.LoadResult.Status.LOCKED) {
                String msg = ChatColor.translateAlternateColorCodes('&',
                    plugin.getConfigManager().getLockTimeoutKickMessage());
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, msg);
            } else {
                plugin.getLogger().warning("Failed to load data for " + uuid + ": " + result.getMessage());
                String msg = ChatColor.translateAlternateColorCodes('&',
                    plugin.getConfigManager().getLoadFailKickMessage());
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, msg);
            }
        }
    }

    /**
     * Apply loaded player data when the player joins.
     * Must be LOWEST priority so data is applied before other plugins interact.
     *
     * <p>On Folia, PlayerJoinEvent fires on the region thread that owns the
     * player entity, but we dispatch via entity scheduler to be explicit
     * and safe across all server implementations.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onJoin(PlayerJoinEvent event) {
        var player = event.getPlayer();
        SchedulerUtil.runAtEntity(plugin, player, () ->
            syncManager.applyPlayerData(player)
        , null);
    }

    /**
     * Collect and save player data when the player quits.
     * Uses HIGHEST priority so other plugins can process quit first.
     *
     * <p>On Folia, Bukkit API (inventory, health, stats) must be read on the
     * player's entity region thread. PlayerQuitEvent may not guarantee this,
     * so we dispatch via entity scheduler.
     */
    @EventHandler(priority = EventPriority.HIGHEST)
    public void onQuit(PlayerQuitEvent event) {
        var player = event.getPlayer();
        SchedulerUtil.runAtEntity(plugin, player, () ->
            syncManager.collectAndSavePlayerData(player)
        , null);
    }
}
