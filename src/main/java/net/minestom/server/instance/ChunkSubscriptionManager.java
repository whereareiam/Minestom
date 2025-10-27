package net.minestom.server.instance;

import net.minestom.server.coordinate.ChunkRange;
import net.minestom.server.coordinate.CoordConversion;
import net.minestom.server.entity.Player;
import space.vectrix.flare.fastutil.Long2ObjectSyncMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages chunk subscriptions for an instance.
 * Players subscribe to chunks they want to view, and the manager handles loading/unloading.
 * <p>
 * Uses concurrent collections for lock-free reads and fine-grained writes.
 */
public final class ChunkSubscriptionManager {
    private final Instance instance;

    // Concurrent collections - no global lock needed
    // Which players want which chunk (chunk index -> players)
    private final Long2ObjectSyncMap<Set<Player>> chunkSubscribers = Long2ObjectSyncMap.hashmap();

    // Which chunks does each player want (player -> chunk indices)
    private final Map<Player, Set<Long>> playerSubscriptions = new ConcurrentHashMap<>();

    public ChunkSubscriptionManager(Instance instance) {
        this.instance = instance;
    }

    /**
     * Subscribe a player to a chunk. Loads the chunk if needed and notifies the player when ready.
     */
    public void subscribe(Player player, int chunkX, int chunkZ) {
        long chunkIndex = CoordConversion.chunkIndex(chunkX, chunkZ);

        // Concurrent collections - no global lock needed
        Set<Player> subscribers = chunkSubscribers.computeIfAbsent(chunkIndex, _ -> ConcurrentHashMap.newKeySet());
        subscribers.add(player);

        Set<Long> playerSubs = playerSubscriptions.computeIfAbsent(player, _ -> ConcurrentHashMap.newKeySet());
        playerSubs.add(chunkIndex);

        // Load chunk and notify player when ready
        instance.loadOptionalChunk(chunkX, chunkZ).thenAccept(chunk -> {
            // Check if still subscribed when load completes (lock-free)
            if (subscribers.contains(player)) {
                player.onChunkReady(chunk);
            }
        });
    }

    /**
     * Unsubscribe a player from a chunk. Unloads the chunk if no more subscribers.
     */
    public void unsubscribe(Player player, int chunkX, int chunkZ) {
        long chunkIndex = CoordConversion.chunkIndex(chunkX, chunkZ);

        // Remove from chunk subscribers (lock-free)
        Set<Player> subscribers = chunkSubscribers.get(chunkIndex);
        boolean wasSubscribed = subscribers.remove(player);

        // Remove from player subscriptions
        Set<Long> playerSubs = playerSubscriptions.get(player);
        if (playerSubs != null) {
            playerSubs.remove(chunkIndex);
        }

        // Clean up empty entries
        if (subscribers.isEmpty()) {
            chunkSubscribers.remove(chunkIndex);
        }

        if (wasSubscribed) {
            player.onChunkRemoved(chunkX, chunkZ);
        }
    }

    /**
     * Update subscriptions for a player based on their position.
     * Called when player changes chunks.
     */
    public void updateSubscriptions(Player player, int centerX, int centerZ, int viewDistance) {
        // Calculate desired chunks
        Set<Long> desiredChunks = ConcurrentHashMap.newKeySet();
        ChunkRange.chunksInRange(centerX, centerZ, viewDistance, (chunkX, chunkZ) ->
                desiredChunks.add(CoordConversion.chunkIndex(chunkX, chunkZ))
        );

        // Get current subscriptions (lock-free)
        Set<Long> currentSubscriptions = playerSubscriptions.getOrDefault(player, Set.of());

        // Unsubscribe from chunks no longer needed
        for (long chunkIndex : currentSubscriptions) {
            if (!desiredChunks.contains(chunkIndex)) {
                int chunkX = CoordConversion.chunkIndexGetX(chunkIndex);
                int chunkZ = CoordConversion.chunkIndexGetZ(chunkIndex);
                unsubscribe(player, chunkX, chunkZ);
            }
        }

        // Subscribe to new chunks
        for (long chunkIndex : desiredChunks) {
            if (!currentSubscriptions.contains(chunkIndex)) {
                int chunkX = CoordConversion.chunkIndexGetX(chunkIndex);
                int chunkZ = CoordConversion.chunkIndexGetZ(chunkIndex);
                subscribe(player, chunkX, chunkZ);
            }
        }
    }

    /**
     * Remove all subscriptions for a player (called when player leaves).
     */
    void unsubscribeAll(Player player) {
        // Remove player subscriptions (lock-free)
        Set<Long> subscriptions = playerSubscriptions.remove(player);

        if (subscriptions != null) {
            for (long chunkIndex : subscriptions) {
                Set<Player> subscribers = chunkSubscribers.get(chunkIndex);
                subscribers.remove(player);
                if (subscribers.isEmpty()) {
                    chunkSubscribers.remove(chunkIndex);
                }
            }
        }
    }

    /**
     * Check if a player is subscribed to a chunk (lock-free read).
     */
    public boolean isSubscribed(Player player, int chunkX, int chunkZ) {
        long chunkIndex = CoordConversion.chunkIndex(chunkX, chunkZ);
        Set<Player> subscribers = chunkSubscribers.get(chunkIndex);
        return subscribers.contains(player);
    }

    /**
     * Get all chunk subscriptions for a player (lock-free read).
     * Returns the live set - do not modify!
     */
    public Set<Long> getPlayerSubscriptions(Player player) {
        Set<Long> subscriptions = playerSubscriptions.get(player);
        return subscriptions != null ? subscriptions : Set.of();
    }
    
    /**
     * Get all players subscribed to a chunk (lock-free read).
     * Returns the live set - do not modify!
     */
    public Set<Player> getChunkSubscribers(int chunkX, int chunkZ) {
        long chunkIndex = CoordConversion.chunkIndex(chunkX, chunkZ);
        Set<Player> subscribers = chunkSubscribers.get(chunkIndex);
        return subscribers != null ? subscribers : Set.of();
    }
}
