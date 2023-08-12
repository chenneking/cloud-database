package org.praktikum.storage;

import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.cache.Cache;
import org.praktikum.storage.cache.FIFOCache;
import org.praktikum.storage.cache.LFUCache;
import org.praktikum.storage.cache.LRUCache;

import java.security.NoSuchAlgorithmException;

public class KVStore {
    public Cache getCache() {
        return cache;
    }

    private Cache cache;
    private final PersistentStorage persistentStorage;

    private final String displacementStrategy;
    private final String storageLocation;
    private final String filename;

    /**
     * Constructs a new KVStore with the specified cache size, displacement strategy, storage location, and filename.
     *
     * @param cacheSize            The maximum size of the cache.
     * @param displacementStrategy The strategy to use for cache displacement (e.g., "FIFO", "LRU", "LFU").
     * @param storageLocation      The location where the persistent storage is found.
     * @param filename             The filename used for the persistent storage.
     */
    public KVStore(int cacheSize, String displacementStrategy, String storageLocation, String filename) {
        persistentStorage = new PersistentStorage(storageLocation, filename);
        switch (displacementStrategy) {
            case "FIFO" -> cache = new FIFOCache(cacheSize, persistentStorage);
            case "LRU" -> cache = new LRUCache(cacheSize, persistentStorage);
            case "LFU" -> cache = new LFUCache(cacheSize, persistentStorage);
        }
        this.displacementStrategy = displacementStrategy;
        this.storageLocation = storageLocation;
        this.filename = filename;
    }

    /**
     * Inserts or updates a key-value pair in the store.
     *
     * @param key   The key to be stored.
     * @param value The associated value.
     * @return The result of the put operation.
     */
    public PutResult put(String key, String value) {
        return cache.put(key, value);
    }

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key The key for which to retrieve the value.
     * @return The value associated with the key, or null if the key is not found.
     */
    public String get(String key) {
        KVPair<String, String> result = cache.get(key);
        if (result != null) {
            return result.getValue();
        }
        return null;
    }

    /**
     * Removes the key-value pair associated with the provided key.
     *
     * @param key The key to be removed.
     * @return The value that was associated with the key, or null if the key was not found.
     */
    public String delete(String key) {
        KVPair<String, String> result = cache.delete(key);
        if (result != null) {
            return result.getValue();
        }
        return null;
    }


    /**
     * Clears all entries from the cache.
     */
    public void flushCache() {
        cache.flushCache();
    }

    /**
     * Clears the cache and retrieves all data from the persistent storage.
     *
     * @return A string representation of all data in the store.
     */
    public String getAllData() {
        cache.flushCache();
        return persistentStorage.getAllData();
    }

    /**
     * Retrieves data between specified key ranges from the persistent storage.
     *
     * @param startKeyRange     The start key of the range.
     * @param keyRangeToSplitAt The end key of the range.
     * @return A string representation of the data within the specified key range.
     */
    public String getDataBetweenKeyRanges(String startKeyRange, String keyRangeToSplitAt) {
        cache.flushCache();
        try {
            ConsistentHashing consistentHashing = new ConsistentHashing();
            return persistentStorage.getDataBetweenKeyRanges(startKeyRange, keyRangeToSplitAt, consistentHashing);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Saves the provided data to the persistent storage.
     *
     * @param data   The data to be saved.
     * @param append If true, appends the data to existing data; otherwise, overwrites existing data.
     */
    public void saveData(String data, boolean append) {
        persistentStorage.saveData(data, append);
    }

    /**
     * Clears all entries from the persistent storage.
     */
    public void cleanPersistentStorage() {
        persistentStorage.clearFile();
    }

    public String getDisplacementStrategy() {
        return displacementStrategy;
    }

    public String getStorageLocation() {
        return storageLocation;
    }

    public String getFilename() {
        return filename;
    }

    /**
     * Deletes all data from the persistent storage.
     *
     * @return true if successful; false otherwise.
     */
    public boolean deleteAllData() {
        return persistentStorage.deleteFile();
    }


}
