package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.LinkedHashMap;


public class LRUCache extends Cache {

    /**
     * Constructs a new LRUCache instance with the specified maximum size and a persistent storage mechanism.
     *
     * @param maxSize           Maximum size for the cache.
     * @param persistentStorage The persistent storage mechanism to interact with.
     */
    public LRUCache(int maxSize, PersistentStorage persistentStorage) {
        super(maxSize, persistentStorage);
    }

    public LinkedHashMap<String, String> activityLog = new LinkedHashMap<>();

    /**
     * Logs the most recent access of the provided key-value pair.
     *
     * @param key   The key of the accessed item.
     * @param value The value of the accessed item.
     */
    private void addActivity(String key, String value) {
        activityLog.remove(key, value);
        activityLog.put(key, value);
    }

    /**
     * Retrieves the value associated with the provided key from the cache or persistent storage.
     * If the key is found in the cache, it updates the LRU log.
     * If the key is not present in the cache but found in the persistent storage,
     * it brings the key-value pair into the cache and evicts the least recently used entry if necessary.
     *
     * @param key A string representing the key associated with the desired value.
     * @return KVPair<String, String> representing the key-value pair if the key is found,
     * or null if the key is not found in either the cache or persistent storage.
     */
    @Override
    public KVPair<String, String> get(String key) {
        String value = hashMap.get(key);
        if (value != null) {
            addActivity(key, value);
            return new KVPair<>(key, value);
        } else {
            KVPair<String, String> kvPair = persistentStorage.get(key);
            //kv pair is null since we cant get the value from the persistent storage
            if (kvPair != null) {
                if (hashMap.size() == maxSize) {
                    String keyToDisplace = activityLog.entrySet().iterator().next().getKey();
                    activityLog.remove(keyToDisplace);
                    // check if they key that's supposed to be displaced has already been displaced
                    if (hashMap.get(keyToDisplace) != null) {
                        String valueToDisplace = hashMap.remove(key);
                        persistentStorage.put(keyToDisplace, valueToDisplace);
                    }
                }
                hashMap.put(kvPair.getKey(), kvPair.getValue());
                addActivity(kvPair.getKey(), kvPair.getValue());
            }
            return kvPair;
        }

    }

    /**
     * Stores the provided key-value pair in the cache and in the persistent storage.
     * If the cache reaches its capacity, it evicts the least recently used entry.
     * It then updates the LRU log for the provided key.
     *
     * @param key   The key to be stored.
     * @param value The value to be associated with the key.
     * @return PutResult indicating the result of the operation in the persistent storage.
     */
    @Override
    public PutResult put(String key, String value) {
        //edge case if the cache-size = 0
        if (maxSize == 0) {
            return persistentStorage.put(key, value);
        }

        //Case: Cache contains key
        if (hashMap.containsKey(key)) {
            hashMap.put(key, value);
            addActivity(key, value);
            return persistentStorage.put(key, value);
        }

        //Case: Cache isn't full & doesn't contain key
        else if (hashMap.size() < maxSize) {
            hashMap.put(key, value);
            addActivity(key, value);
            return persistentStorage.put(key, value);
        }
        //Case: Cache is full and we need to replace one value with our selected value
        else {
            String keyToDisplace = activityLog.entrySet().iterator().next().getKey();
            activityLog.remove(keyToDisplace);
            // check if they key that's supposed to be displaced has already been displaced
            String valueToDisplace = hashMap.get(keyToDisplace);
            if (valueToDisplace != null) {
                hashMap.remove(keyToDisplace);
                persistentStorage.put(keyToDisplace, valueToDisplace);

            }
        }

        hashMap.put(key, value);
        //update activity list
        addActivity(key, value);
        return persistentStorage.put(key, value);
    }

}