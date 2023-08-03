package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.LinkedList;
import java.util.Queue;

public class FIFOCache extends Cache {


    private final LinkedList<String> insertionOrder = new LinkedList<>();

    public FIFOCache(int maxSize, PersistentStorage persistentStorage) {
        super(maxSize, persistentStorage);
    }

    /**
     * Retrieves the value associated with the provided key.
     *
     * @param key A string representing the key associated with the desired value.
     * @return KVPair<String, String> representing the key-value pair if the key is found,
     * or null if the key is not found in either the cache
     */
    @Override
    public KVPair<String, String> get(String key) {
        String value = hashMap.get(key);
        if (value != null) {
            return new KVPair<>(key, value);
        }
        else {
            KVPair<String, String> kvPair = persistentStorage.get(key);
            //kv pair is null since we can't get the value from the persistent storage
            if (kvPair != null) {
                if (hashMap.size() == maxSize) {
                    if (insertionOrder.size() >= 1) {
                        String keyToDisplace = insertionOrder.remove();
                        // check if the key that's supposed to be displaced has already been displaced
                        if (hashMap.get(keyToDisplace) != null) {
                            String valueToDisplace = hashMap.remove(keyToDisplace);
                            persistentStorage.put(keyToDisplace, valueToDisplace);
                        }
                    }
                }
                hashMap.put(kvPair.getKey(), kvPair.getValue());
            }
            return kvPair;
        }
    }

    /**
     * Stores the key-value pair in the cache and persistent storage, utilizing the FIFOStrategy.
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

        if (hashMap.containsKey(key)) {
            hashMap.put(key, value);
            return persistentStorage.put(key, value);
        }

        //Case: Cache isn't full & doesn't contain key
        else if (hashMap.size() < maxSize) {
            hashMap.put(key, value);
            insertionOrder.add(key);
            return persistentStorage.put(key, value);
        }

        //check if cache is full
        else {
            String keyToDisplace = insertionOrder.removeFirst();
            // check if they key that's supposed to be displaced has already been displaced
            String valueToDisplace = hashMap.get(keyToDisplace);
            if (valueToDisplace != null) {
                persistentStorage.put(keyToDisplace, valueToDisplace);
            }
        }


        hashMap.put(key, value);
        //update activity list
        insertionOrder.add(key);
        return persistentStorage.put(key, value);
    }
}