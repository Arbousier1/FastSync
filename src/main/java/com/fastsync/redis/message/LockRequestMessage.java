package com.fastsync.redis.message;

import com.fastsync.redis.RedisManager;
import io.netty.buffer.ByteBuf;
import net.momirealms.sparrow.redis.messagebroker.MessageIdentifier;
import net.momirealms.sparrow.redis.messagebroker.RedisMessage;
import net.momirealms.sparrow.redis.messagebroker.codec.MessageCodec;
import net.momirealms.sparrow.redis.messagebroker.message.OneWayMessage;
import net.momirealms.sparrow.redis.messagebroker.util.ByteBufHelper;

/**
 * One-way broadcast message: a server is requesting that the server currently
 * holding the lock for a player releases it as soon as it finishes saving.
 *
 * <p>This is a broadcast ({@code targetServer = ""}) sent to all servers. The
 * server that owns the lock cannot release it immediately (it is still saving),
 * but once {@code saveData()} completes it will publish a
 * {@link LockReleasedMessage} so the waiting server can retry instantly instead
 * of blindly polling.
 *
 * <p>Wire layout (after the broker's identifier header):
 * <pre>
 *   targetServer  : UTF-8 string  (written/read by OneWayMessage, max 32767)
 *   playerUuid    : UTF-8 string  (written/read by this class, max 32767)
 * </pre>
 */
public class LockRequestMessage extends OneWayMessage<ByteBuf> {

    /** Identifier under which this message is registered on the broker. */
    public static final MessageIdentifier IDENTIFIER = MessageIdentifier.of("fastsync", "lock_request");

    /** Codec used to (de)serialize this message on the broker. */
    public static final MessageCodec<ByteBuf, LockRequestMessage> CODEC =
        RedisMessage.codec((msg, buf) -> msg.write(buf), LockRequestMessage::new);

    private String playerUuid;

    /**
     * No-arg constructor, available for the decoder/reflection.
     */
    public LockRequestMessage() {
        // fields are populated by the buffer constructor when decoding
    }

    /**
     * Creates a broadcast lock-request message for the given player.
     *
     * @param playerUuid the player UUID whose lock should be released
     */
    public LockRequestMessage(String playerUuid) {
        this.playerUuid = playerUuid;
        this.targetServer = ""; // broadcast to every server
    }

    /**
     * Decoder constructor: reads {@code targetServer} (via the parent) and then
     * {@code playerUuid} from the buffer.
     *
     * @param buf the incoming message buffer
     */
    public LockRequestMessage(ByteBuf buf) {
        super(buf);
        this.playerUuid = ByteBufHelper.readUtf8(buf, 32767);
    }

    @Override
    protected void write(ByteBuf buf) {
        super.write(buf);
        ByteBufHelper.writeUtf8(buf, this.playerUuid, 32767);
    }

    @Override
    public MessageIdentifier identifier() {
        return IDENTIFIER;
    }

    @Override
    protected void handle() {
        RedisManager.handleLockRequest(playerUuid);
    }

    public String getPlayerUuid() {
        return playerUuid;
    }
}
