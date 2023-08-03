package org.praktikum.storage.cache;

import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;
import org.praktikum.storage.PersistentStorage;

import java.util.HashMap;
import java.util.LinkedHashSet;


public class LFUCache extends Cache {

    private final HashMap<String, Integer> keyMap = new HashMap();
    private final HashMap<Integer, LinkedHashSet<String>> frequencyMap = new HashMap<>();

    public LFUCache(int maxSize, PersistentStorage persistentStorage) {
        super(maxSize, persistentStorage);
    }


    /**
     * Retrieves and removes the least frequently used key from the frequency map.
     * The method starts from the lowest frequency count and searches for the
     * keys associated with it.
     *
     * @return String representing the least frequently used key or null if the key map is empty.
     */
    @Override
    public KVPair<String, String> get(String key) {
        if (hashMap.containsKey(key)) {
            increaseActivity(key);
            return new KVPair<>(key, hashMap.get(key));
        }
        else {
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
     * Method used to increase the activity count of a specific key.
     * Assumes key is initialized due to being inserted previously.
     *
     * @param key of Key-Value entry
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
        if (! frequencyMap.containsKey(val + 1)) {
            newFreqKeySet = new LinkedHashSet<>();
        }
        else {
            newFreqKeySet = frequencyMap.get(val + 1);
        }
        newFreqKeySet.add(key);
        frequencyMap.put(val + 1, newFreqKeySet);
    }

    /**
     * Retrieves and removes the least frequently used key from the frequency map.
     * The method starts from the lowest frequency count and searches for the
     * keys associated with it. If such keys exist, one key is returned and removed.
     * If not, it proceeds to the next frequency count. If the key map is empty,
     * the method returns null.
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
     * Evicts the least frequently used (LFU) entry from the cache and stores it to the persistent storage.
     * This method is invoked when the cache is full and needs to accommodate a new entry. It identifies
     * the LFU key, removes it from the cache, and stores it to the persistent storage.
     */

    private void displaceLFUEntryToFile() {
        String leastFreqUsedKey = getAndRemoveLeastFrequentlyUsedKey();
        String leastFreqUsedValue = hashMap.get(leastFreqUsedKey);
        hashMap.remove(leastFreqUsedKey);
        persistentStorage.put(leastFreqUsedKey, leastFreqUsedValue);
    }

    /**
     * Initializes the provided key in the cache, setting its frequency to zero.
     *
     * @param key the key to be initialized in the cache.
     */

    private void initializeKey(String key) {
        if (! keyMap.containsKey(key)) {
            keyMap.put(key, 0);
            LinkedHashSet<String> freqKeySet;
            if (! frequencyMap.containsKey(0)) {
                freqKeySet = new LinkedHashSet<>();
            }
            else {
                freqKeySet = frequencyMap.get(0);
            }
            freqKeySet.add(key);
            frequencyMap.put(0, freqKeySet);
        }
    }

    /**
     * Puts the provided key-value pair in the cache and or persistent storage.
     *
     * @param key   the key to be stored.
     * @param value the value to be associated with the key.
     * @return PutResult indicating the result of the put operation in the persistent storage.
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
