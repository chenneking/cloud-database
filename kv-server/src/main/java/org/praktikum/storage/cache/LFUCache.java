package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.HashMap;
import java.util.LinkedHashSet;


public class LFUCache extends Cache {

    private final HashMap<String, Integer> keyMap = new HashMap();
    private final HashMap<Integer, LinkedHashSet<String>> frequencyMap = new HashMap<>();

    /**
     * Constructs a new LFUCache instance with the specified maximum size and a persistent storage mechanism.
     *
     * @param maxSize           Maximum size for the cache.
     * @param persistentStorage The persistent storage mechanism to interact with.
     */
    public LFUCache(int maxSize, PersistentStorage persistentStorage) {
        super(maxSize, persistentStorage);
    }


    /**
     * Retrieves the value associated with the provided key from the cache or persistent storage.
     * It updates the access frequency of the key.
     * If the key is not present in the cache but found in the persistent storage,
     * it brings the key-value pair into the cache and evicts the least frequently used entry if necessary.
     *
     * @param key A string representing the key associated with the desired value.
     * @return KVPair<String, String> representing the key-value pair if the key is found,
     * or null if the key is not found in either the cache or persistent storage.
     */
    @Override
    public KVPair<String, String> get(String key) {
        if (hashMap.containsKey(key)) {
            increaseActivity(key);
            return new KVPair<>(key, hashMap.get(key));
        } else {
            KVPair<String, String> kvPair = persistentStorage.get(key);
            //kv pair is null since we cant get the value from the persistent storage
            if (kvPair != null) {
                keyMap.put(kvPair.getKey(), 0);
                increaseActivity(key);
                if (hashMap.size() == maxSize) {
                    displaceLFUEntryToFile();
                }

                hashMap.put(kvPair.getKey(), kvPair.getValue());
            }
            return kvPair;
        }
    }


    /**
     * Increases the frequency of access for the specified key.
     * If the key is new, it initializes its frequency.
     *
     * @param key A string representing the key whose access frequency needs to be increased.
     */
    private void increaseActivity(String key) {
        Integer val = keyMap.get(key);
        keyMap.put(key, val + 1);

        //remove key from old frequency set
        LinkedHashSet<String> oldFreqKeySet = frequencyMap.get(val);
        oldFreqKeySet.remove(key);
        frequencyMap.put(val, oldFreqKeySet);

        //add key to new frequency set
        LinkedHashSet<String> newFreqKeySet;
        if (!frequencyMap.containsKey(val + 1)) {
            newFreqKeySet = new LinkedHashSet<>();
        } else {
            newFreqKeySet = frequencyMap.get(val + 1);
        }
        newFreqKeySet.add(key);
        frequencyMap.put(val + 1, newFreqKeySet);
    }

    /**
     * Identifies and removes the least frequently used key from the cache.
     *
     * @return String representing the least frequently used key or null if the key map is empty.
     */
    private String getAndRemoveLeastFrequentlyUsedKey() {
        if (keyMap.size() == 0) {
            return null;
        }
        //get one key from the least frequently used ones
        int counter = 0;

        while (true) {
            LinkedHashSet<String> possibleKeys = frequencyMap.get(counter++);
            if (possibleKeys != null) {
                String key = possibleKeys.iterator().next();
                possibleKeys.remove(key);
                return key;
            }
        }
    }

    /**
     * Evicts the least frequently used entry from the cache and pushes it to the persistent storage.
     */
    private void displaceLFUEntryToFile() {
        String leastFreqUsedKey = getAndRemoveLeastFrequentlyUsedKey();
        String leastFreqUsedValue = hashMap.get(leastFreqUsedKey);
        hashMap.remove(leastFreqUsedKey);
        persistentStorage.put(leastFreqUsedKey, leastFreqUsedValue);
    }

    /**
     * Initializes the frequency of a new key to zero.
     *
     * @param key A string representing the key to be initialized.
     */
    private void initializeKey(String key) {
        if (!keyMap.containsKey(key)) {
            keyMap.put(key, 0);
            LinkedHashSet<String> freqKeySet;
            if (!frequencyMap.containsKey(0)) {
                freqKeySet = new LinkedHashSet<>();
            } else {
                freqKeySet = frequencyMap.get(0);
            }
            freqKeySet.add(key);
            frequencyMap.put(0, freqKeySet);
        }
    }

    /**
     * Stores the provided key-value pair in the cache and in the persistent storage.
     * If the cache reaches its capacity, it evicts the least frequently used entry.
     *
     * @param key   The key to be stored.
     * @param value The value to be associated with the key.
     * @return PutResult indicating the result of the operation in the persistent storage.
     */
    @Override
    public PutResult put(String key, String value) {
        if (maxSize == 0) {
            return persistentStorage.put(key, value);
        }

        if (hashMap.containsKey(key)) {
            hashMap.put(key, value);
            increaseActivity(key);
            return persistentStorage.put(key, value);
        }

        if (hashMap.size() == maxSize) {
            displaceLFUEntryToFile();
        }
        hashMap.put(key, value);
        initializeKey(key);
        return persistentStorage.put(key, value);
    }

}
