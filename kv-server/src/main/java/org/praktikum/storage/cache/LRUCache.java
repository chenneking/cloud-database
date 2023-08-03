package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.LinkedHashMap;


public class LRUCache extends Cache {


    public LRUCache(int maxSize, PersistentStorage persistentStorage) {
        super(maxSize, persistentStorage);
    }

    public LinkedHashMap<String, String> activityLog = new LinkedHashMap<>();


    private void addActivity(String key, String value) {
        activityLog.remove(key, value);
        activityLog.put(key, value);
    }

    /**
     * Retrieves the value corresponding to the given key from the cache or persistent storage via LRU strategy.
     *
     * @param key the key to be searched for.
     * @return KVPair representing the key-value pair if it exists in the cache or persistent storage; null otherwise.
     */

    @Override
    public KVPair<String, String> get(String key) {
        String value = hashMap.get(key);
        if (value != null) {
            addActivity(key, value);
            return new KVPair<>(key, value);
        }
        else {
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
     * Stores the key-value pair in the cache or if the cache is full in the persistent storage , utilizing the LRU strategy.
     *
     * @param key   the key to be stored.
     * @param value the value to be stored.
     * @return PutResult representing the status of the put operation in the persistent storage.
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