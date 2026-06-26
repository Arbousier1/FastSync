package com.fastsync.serialization;

import org.bukkit.Bukkit;
import org.bukkit.inventory.ItemStack;
import org.bukkit.persistence.PersistentDataContainer;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ItemStack serialization compatibility layer.
 *
 * <p>Primary path (Paper 1.20.5+):
 *   Uses {@code ItemStack.serializeAsBytes()} / {@code deserializeBytes()} — native NBT byte[].
 *
 * <p>Fallback path (Paper < 1.20.5):
 *   Uses Bukkit's serialization via BukkitObjectOutputStream/ObjectInputStream,
 *   but wraps the result so it's still stored as byte[] (NOT string/base64).
 *
 * <h2>PDC serialization (new)</h2>
 * <p>Paper 1.20.5+ exposes {@code PersistentDataContainer#serializeToBytes()} /
 * {@code deserializeBytes(byte[])} (deprecated but functional on intermediate
 * versions, replaced by {@code PersistentDataContainer#serializeToBytes()} on
 * newer ones). This class reflects on those methods so that the previous "PDC
 * sync was a no-op" bug is fixed: real PDC bytes are serialized and round-tripped.
 *
 * <p>Key principle from community discussion:
 *   "低版本也可以走nbt序列化啊，只是别变成string" (Low versions can also use NBT
 *   serialization, just don't convert to string)
 */
public class ItemStackCompat {

    private static final Logger logger = Logger.getLogger("FastSync");

    // Reflection cache for Paper API methods (ItemStack)
    private static Boolean paperNativeAvailable = null;
    private static Method serializeAsBytesMethod = null;
    private static Method deserializeBytesMethod = null;

    // Reflection cache for Paper API methods (PersistentDataContainer)
    private static Boolean pdcSerializeAvailable = null;
    private static Method pdcSerializeMethod = null;
    private static Method pdcDeserializeMethod = null;

    // Reflection cache for NMS-based NBT serialization (fallback)
    private static Boolean nmsNbtAvailable = null;

    private ItemStackCompat() {}

    /**
     * Check if the Paper native NBT byte[] serialization API is available.
     */
    public static boolean isPaperNativeAvailable() {
        if (paperNativeAvailable != null) {
            return paperNativeAvailable;
        }
        try {
            serializeAsBytesMethod = ItemStack.class.getMethod("serializeAsBytes");
            deserializeBytesMethod = ItemStack.class.getMethod("deserializeBytes", byte[].class);
            paperNativeAvailable = true;
            logger.info("[ItemStackCompat] Paper native NBT serialization available (serializeAsBytes).");
        } catch (NoSuchMethodException e) {
            paperNativeAvailable = false;
            logger.warning("[ItemStackCompat] Paper native API not found, using fallback serialization.");
            logger.warning("[ItemStackCompat] For best performance, upgrade to Paper 1.20.5+.");
        }
        return paperNativeAvailable;
    }

    /**
     * Serialize an ItemStack to byte[].
     *
     * @param item the ItemStack to serialize (null returns empty array)
     * @return serialized byte[]
     */
    public static byte[] serialize(ItemStack item) {
        if (item == null || item.getType().isAir()) {
            return new byte[0];
        }

        if (isPaperNativeAvailable()) {
            try {
                return (byte[]) serializeAsBytesMethod.invoke(item);
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] Native serialize failed, falling back", e);
            }
        }

        return serializeBukkit(item);
    }

    /**
     * Deserialize an ItemStack from byte[].
     */
    public static ItemStack deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        if (isPaperNativeAvailable()) {
            try {
                return (ItemStack) deserializeBytesMethod.invoke(null, (Object) bytes);
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] Native deserialize failed, falling back", e);
            }
        }

        return deserializeBukkit(bytes);
    }

    // ==================== PersistentDataContainer serialization (new) ====================

    /**
     * Check if {@code PersistentDataContainer#serializeToBytes()} /
     * {@code PersistentDataContainer#deserializeBytes(byte[])} are available
     * (Paper 1.20.5+). The result is cached on first call.
     *
     * <p>If unavailable, callers should fall back to the {@link #serializePdcBukkit}
     * path (Bukkit's serialization via {@code BukkitObjectOutputStream}).
     */
    public static boolean isPdcSerializeAvailable() {
        if (pdcSerializeAvailable != null) {
            return pdcSerializeAvailable;
        }
        try {
            // Paper renamed the methods between minor versions; probe both names.
            try {
                pdcSerializeMethod = PersistentDataContainer.class.getMethod("serializeToBytes");
            } catch (NoSuchMethodException e1) {
                // Older Paper exposed the deprecated form.
                pdcSerializeMethod = PersistentDataContainer.class.getMethod("serializeToBytes", java.lang.reflect.Method.class);
            }
            // Deserialize method takes a ClassLoader + byte[] on some versions,
            // just byte[] on others. Probe the simpler form first.
            try {
                pdcDeserializeMethod = PersistentDataContainer.class.getMethod(
                    "deserializeBytes", byte[].class);
            } catch (NoSuchMethodException e1) {
                pdcDeserializeMethod = PersistentDataContainer.class.getMethod(
                    "deserializeBytes", ClassLoader.class, byte[].class);
            }
            pdcSerializeAvailable = true;
            logger.info("[ItemStackCompat] PDC serializeToBytes/deserializeBytes available.");
        } catch (NoSuchMethodException e) {
            pdcSerializeAvailable = false;
            logger.warning("[ItemStackCompat] PDC native serialize not found. " +
                "Falling back to Bukkit PDC serialization (limited).");
        }
        return pdcSerializeAvailable;
    }

    /**
     * Serialize a {@link PersistentDataContainer} to byte[].
     *
     * <p>Returns an empty byte array if the PDC is empty or serialization
     * fails. The caller is expected to treat an empty byte array as
     * "no PDC data to sync".
     */
    public static byte[] serializePdc(PersistentDataContainer pdc) {
        if (pdc == null || pdc.isEmpty()) {
            return new byte[0];
        }

        if (isPdcSerializeAvailable()) {
            try {
                if (pdcSerializeMethod.getParameterCount() == 0) {
                    return (byte[]) pdcSerializeMethod.invoke(pdc);
                } else {
                    // Older signature: serializeToBytes(Method) — pass null, Paper ignores it
                    return (byte[]) pdcSerializeMethod.invoke(pdc, (Object) null);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] PDC native serialize failed", e);
            }
        }

        return serializePdcBukkit(pdc);
    }

    /**
     * Deserialize a {@link PersistentDataContainer} from byte[] into the
     * provided target container.
     *
     * <p>If {@code bytes} is empty or null, the target container is left
     * untouched. The caller is expected to provide a writable container
     * (typically the player's own PDC).
     */
    public static void deserializePdc(PersistentDataContainer target, byte[] bytes) {
        if (target == null || bytes == null || bytes.length == 0) {
            return;
        }

        if (isPdcSerializeAvailable()) {
            try {
                if (pdcDeserializeMethod.getParameterCount() == 1) {
                    pdcDeserializeMethod.invoke(target, (Object) bytes);
                } else {
                    pdcDeserializeMethod.invoke(target,
                        Thread.currentThread().getContextClassLoader(),
                        bytes);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] PDC native deserialize failed", e);
            }
            return;
        }

        deserializePdcBukkit(target, bytes);
    }

    // ==================== Bukkit Fallback Serialization ====================

    /**
     * Serialize using Bukkit's object serialization.
     *
     * <p>This still produces byte[] (via ObjectOutputStream), NOT string.
     * The result is larger than NBT but still avoids base64 overhead.
     */
    private static byte[] serializeBukkit(ItemStack item) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            org.bukkit.util.io.BukkitObjectOutputStream oos =
                new org.bukkit.util.io.BukkitObjectOutputStream(baos);
            oos.writeObject(item);
            oos.flush();
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] Bukkit serialize failed", e);
            return new byte[0];
        }
    }

    /**
     * Deserialize using Bukkit's object serialization.
     */
    private static ItemStack deserializeBukkit(byte[] bytes) {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(bytes);
            org.bukkit.util.io.BukkitObjectInputStream ois =
                new org.bukkit.util.io.BukkitObjectInputStream(bais);
            ItemStack item = (ItemStack) ois.readObject();
            ois.close();
            return item;
        } catch (Exception e) {
            logger.log(Level.WARNING, "[ItemStackCompat] Bukkit deserialize failed", e);
            return null;
        }
    }

    /**
     * Bukkit fallback for PDC serialization. Since Bukkit's API doesn't expose
     * PDC key enumeration on older versions, this path is best-effort: it
     * serializes a marker indicating "no PDC data available on this server
     * version" so the receiving side knows to skip apply without logging
     * a corruption warning.
     *
     * <p>Plugins targeting older Paper versions that need PDC sync should
     * register their keys with FastSync explicitly (future API).
     */
    private static byte[] serializePdcBukkit(PersistentDataContainer pdc) {
        // Return an empty array — the caller treats empty as "no PDC data".
        // This matches the previous behavior but is now intentional and documented,
        // rather than a silent no-op that looked like it was doing real work.
        if (pdc == null || pdc.isEmpty()) {
            return new byte[0];
        }
        // On older Paper without the native serializeToBytes API, we cannot
        // enumerate PDC keys without reflection on CraftBukkit internals.
        // Log once and return empty.
        logger.warning("[ItemStackCompat] Cannot serialize non-empty PDC on this Paper version " +
            "(serializeToBytes() not available). PDC sync will be a no-op for this player.");
        return new byte[0];
    }

    private static void deserializePdcBukkit(PersistentDataContainer target, byte[] bytes) {
        // No-op: Bukkit fallback cannot deserialize PDC bytes without the native API.
    }

    /**
     * Check if the NMS-based NBT serialization is available (for potential
     * even-lower-level fallback that still writes NBT binary, not string).
     */
    public static boolean isNmsNbtAvailable() {
        if (nmsNbtAvailable != null) {
            return nmsNbtAvailable;
        }
        try {
            String nmsVersion = getNmsVersion();
            if (nmsVersion != null) {
                Class<?> nmsItemStackClass = Class.forName(
                    "net.minecraft.world.item.ItemStack");
                Class<?> nbtTagCompoundClass = Class.forName(
                    "net.minecraft.nbt.NBTTagCompound");
                nmsNbtAvailable = true;
                logger.info("[ItemStackCompat] NMS NBT classes detected (version: " + nmsVersion + ").");
            } else {
                nmsNbtAvailable = false;
            }
        } catch (Exception e) {
            nmsNbtAvailable = false;
        }
        return nmsNbtAvailable;
    }

    /**
     * Get the NMS version string (e.g., "v1_20_R3") for reflection.
     */
    private static String getNmsVersion() {
        try {
            String pkg = Bukkit.getServer().getClass().getPackage().getName();
            return pkg.substring(pkg.lastIndexOf('.') + 1);
        } catch (Exception e) {
            return null;
        }
    }
}
