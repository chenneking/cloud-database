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

    public PersistantStorageMultiThreaded(String storageLocation) {
        this.storageLocation = storageLocation;
        initializePersistentStorage();
    }

    /**
     * Retrieves the key-value pair associated with the given key by initiating a StorageThread to perform the get operation.
     *
     * @param key The key of the key-value pair to be retrieved.
     * @return The key-value pair associated with the given key, or null if the key does not exist in the storage.
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
     * Writes or updates the key-value pair associated with the given key and vlaue by initiating a StorageThread to perform the put operation.
     *
     * @param key The key of the key-value pair to be retrieved.
     * @return The key-value pair associated with the given key, or null if the key does not exist in the storage.
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
        }
        else {
            return null;
        }
    }

    /**
     * Retrieves all data by initiating a StorageThread to perform the get operation.
     *
     * @return The key-value pair associated with the given key, or null if the key does not exist in the storage.
     */
    public String getAllData() {
        StorageThread storageThread = new StorageThread("getAll", null, null, storageLocation, FILENAME);
        storageThread.run();
        return storageThread.getCompleteData();

    }

    /**
     * Initializes the persistent storage by ensuring the existence of the storage file.
     *
     * @throws RuntimeException If there's an error during storage file creation.
     */
    //has to create a data file if it does not exist already
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

