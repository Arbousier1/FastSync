package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.PluginManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class SyncManagerApplyIsolationTest {

    @Test
    void clearBeforeApplyDoesNotClearDisabledComponents() throws Exception {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("apply-isolation-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.isClearBeforeApply()).thenReturn(true);
        when(config.isSyncInventory()).thenReturn(false);
        when(config.isSyncEnderChest()).thenReturn(false);
        when(config.isSyncPotionEffects()).thenReturn(false);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);

        PlayerData data = new PlayerData();
        data.setVersion(4);
        data.setFencingToken(8);
        pendingData(manager).put(uuid, data);

        PluginManager pluginManager = mock(PluginManager.class);
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            manager.applyPlayerData(player);
        }

        verify(player, never()).getInventory();
        verify(player, never()).getEnderChest();
        verify(player, never()).getActivePotionEffects();
        verify(pluginManager).callEvent(any());
        assertTrue(manager.isPlayerActive(uuid));
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<UUID, PlayerData> pendingData(SyncManager manager) throws Exception {
        Field field = SyncManager.class.getDeclaredField("pendingData");
        field.setAccessible(true);
        return (ConcurrentHashMap<UUID, PlayerData>) field.get(manager);
    }
}
