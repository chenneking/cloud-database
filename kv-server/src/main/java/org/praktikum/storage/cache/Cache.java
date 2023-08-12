package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.HashMap;
import java.util.Map;

public abstract class Cache {
    protected HashMap<String, String> hashMap;

    public int getMaxSize() {
        return maxSize;
    }

    protected int maxSize;
    protected PersistentStorage persistentStorage;

    /**
     * Constructs a new Cache instance with a specified maximum size and persistent storage.
     *
     * @param maxSize The maximum size for the cache.
     * @param storage The persistent storage mechanism to interact with.
     */
    public Cache(int maxSize, PersistentStorage storage) {
        this.hashMap = new HashMap<>();
        this.maxSize = maxSize;
        this.persistentStorage = storage;
    }

    public abstract KVPair<String, String> get(String key);

    public abstract PutResult put(String key, String value);

    /**
     * Deletes the value associated with the provided key in the cache and/or persistent storage.
     *
     * @param key The key associated with the value to delete.
     * @return A key-value pair representing the deleted entry, or null if the key was not found.
     */
    public KVPair<String, String> delete(String key) {
        String cacheValue = hashMap.remove(key);
        KVPair<String, String> persistentValue = persistentStorage.delete(key);
        if (cacheValue != null) {
            return new KVPair<>(key, cacheValue);
        } else return persistentValue;
    }

    /**
     * Flushes the cache, storing all key-value pairs into the persistent storage.
     */
    public void flushCache() {
        if (hashMap.size() == 0) {
            return;
        }
        for (Map.Entry<String, String> resultMap : hashMap.entrySet()) {
            persistentStorage.put(resultMap.getKey(), resultMap.getValue());
        }
    }

}
