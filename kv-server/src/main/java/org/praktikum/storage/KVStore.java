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
     * puts a key value into the database
     *
     * @param key   String <key>
     * @param value String <value>
     * @return Enum of the result of the put operation
     */
    public PutResult put(String key, String value) {
        return cache.put(key, value);
    }

    /**
     * gets value from the database using the given key
     *
     * @param key String <key>
     * @return String <value> or null if it could not find the <key>
     */
    public String get(String key) {
        KVPair<String, String> result = cache.get(key);
        if (result != null) {
            return result.getValue();
        }
        return null;
    }

    /**
     * deletes value from the database using the given key
     *
     * @param key String <key>
     * @return String <value> or null if it could not delete the object
     */
    public String delete(String key) {
        KVPair<String, String> result = cache.delete(key);
        if (result != null) {
            return result.getValue();
        }
        return null;
    }

    /**
     * Flushes the cache
     */
    public void flushCache() {
        cache.flushCache();
    }

    /**
     * Flushes the cache and gets all data from the persitant storage
     *
     * @return String with all the data
     */
    public String getAllData() {
        cache.flushCache();
        return persistentStorage.getAllData();
    }

    /**
     * Gets the data between the keyranges from the persitant storage
     *
     * @param startKeyRange
     * @param keyRangeToSplitAt
     * @return
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
     * Saves the data to the persistent storage
     *
     * @param data
     * @param append
     */
    public void saveData(String data, boolean append) {
        persistentStorage.saveData(data, append);
    }

    /**
     * Clears the persitant storage
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

    public boolean deleteAllData() {
        return persistentStorage.deleteFile();
    }
}
