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

    public Cache(int maxSize, PersistentStorage storage) {
        this.hashMap = new HashMap<>();
        this.maxSize = maxSize;
        this.persistentStorage = storage;
    }

    public abstract KVPair<String, String> get(String key);

    public abstract PutResult put(String key, String value);

    /**
     * Deletes the value associated to the key in Cache and or in persistent storage
     *
     * @param key associated with the value that is to be deleted
     * @return KVPair<String, String> representing the deleted key-value pair,
     * *         or null if the key was not found in either the cache or persistent storage.
     */
    public KVPair<String, String> delete(String key) {
        String cacheValue = hashMap.remove(key);
        KVPair<String, String> persistentValue = persistentStorage.delete(key);
        if (cacheValue != null) {
            return new KVPair<>(key, cacheValue);
        }
        else return persistentValue;
    }

    /**
     * Flushes the Cache by putting all the KVPairs to the persistent storage
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
