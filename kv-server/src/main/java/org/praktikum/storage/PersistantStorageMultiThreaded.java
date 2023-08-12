package org.praktikum.storage;

import org.praktikum.KVServer;
import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class PersistantStorageMultiThreaded {

    private final String storageLocation;
    private final String FILENAME = "StorageFile.csv";

    /**
     * Constructs a new PersistantStorageMultiThreaded instance.
     *
     * @param storageLocation The directory location where the storage file is located or will be created.
     */
    public PersistantStorageMultiThreaded(String storageLocation) {
        this.storageLocation = storageLocation;
        initializePersistentStorage();
    }

    /**
     * Fetches the value associated with a given key from the storage.
     *
     * @param key The key whose associated value is to be fetched.
     * @return The key-value pair associated with the specified key, or null if the key is not present.
     */
    public KVPair<String, String> get(String key) {
        StorageThread storageThread = new StorageThread("get", key, null, storageLocation, FILENAME);
        storageThread.run();
        //if the value of the storageThread is still null after trying to get the storage object, the object does not exist
        if (storageThread.getValue() == null) {
            return null;
        }
        return new KVPair<>(storageThread.getKey(), storageThread.getValue());
    }

    /**
     * Stores or updates a key-value pair in the storage.
     *
     * @param key   The key to be stored.
     * @param value The value associated with the key.
     * @return The result of the put operation.
     */
    public PutResult put(String key, String value) {
        StorageThread storageThread = new StorageThread("put", key, value, storageLocation, FILENAME);
        storageThread.run();
        return storageThread.getPutResult();
    }

    /**
     * Delets the key-value pair associated with the given key by initiating a StorageThread to perform the get operation.
     *
     * @param key The key of the key-value pair to be retrieved.
     * @return The key-value pair associated with the given key, or null if the key does not exist in the storage.
     */
    // @ToDo check if delete returns the correct values
    public KVPair<String, String> delete(String key) {
        StorageThread storageThread = new StorageThread("delete", key, null, storageLocation, FILENAME);
        storageThread.run();
        String value = storageThread.getValue();
        if (value != null) {
            return new KVPair<>(key, value);
        } else {
            return null;
        }
    }

    /**
     * Fetches all the data present in the storage.
     *
     * @return A string representation of all data in the storage.
     */
    public String getAllData() {
        StorageThread storageThread = new StorageThread("getAll", null, null, storageLocation, FILENAME);
        storageThread.run();
        return storageThread.getCompleteData();

    }

    /**
     * Initializes the persistent storage. This method ensures that the storage file exists.
     * If the file doesn't exist, it will create one.
     *
     * @throws RuntimeException If there's an error during storage file creation.
     */
    public void initializePersistentStorage() {
        try {
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + FILENAME));
        } catch (FileNotFoundException e) {
            try {
                File dir = new File("/" + storageLocation);
                dir.mkdirs();
                File file = new File("/" + storageLocation + "/" + FILENAME);
                file.createNewFile();
            } catch (Exception ex) {
                KVServer.log.warning("Error while trying to create storage file at start-up");
                throw new RuntimeException(ex);
            }
        }
    }

}

