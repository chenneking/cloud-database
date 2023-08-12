package org.praktikum.storage;

import org.praktikum.KVServer;
import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.KVPair;
import org.praktikum.resources.PutResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class PersistentStorage {

    private final String storageLocation;
    private final String filename;

    public PersistentStorage(String storageLocation, String filename) {
        this.storageLocation = storageLocation;
        this.filename = filename;
        initializePersistentStorage();
    }

    /**
     * Fetches the value associated with a given key from the storage.
     *
     * @param key The key whose associated value is to be fetched.
     * @return The key-value pair associated with the specified key, or null if the key is not present.
     */
    public KVPair<String, String> get(String key) {
        try {
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            try {
                String[] output = input.next().split(",");
                KVPair<String, String> kvPair = new KVPair<>(output[0], output[1]);

                while (input.hasNext() & !kvPair.getKey().equals(key)) {
                    output = input.next().split(",");
                    kvPair = new KVPair<>(output[0], output[1]);
                }
                if (kvPair.getKey().equals(key)) {
                    input.close();
                    return kvPair;
                }
                input.close();
                return null;
                //the file is empty
            } catch (NoSuchElementException noSuchElementException) {
                KVServer.log.warning("Error key doesn't exist in persistent storage");
                return null;
            }
        } catch (FileNotFoundException fileNotFoundException) {
            KVServer.log.warning("Error while trying to open non-existent storage file");
            return null;
        }
    }

    /**
     * Stores or updates a key-value pair in the storage.
     *
     * @param key   The key to be stored.
     * @param value The value associated with the key.
     * @return The result of the put operation.
     */
    public PutResult put(String key, String value) {
        try {
            boolean flag = false;
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            LinkedList<KVPair<String, String>> linkedList = new LinkedList<>();

            while (input.hasNext()) {
                String[] output = input.next().split(",");
                try {
                    String keyOutput = output[0];
                    String valueOutput = output[1];

                    KVPair<String, String> kvPair = new KVPair<>(keyOutput, valueOutput);

                    if (kvPair.getKey().equals(key)) {
                        kvPair.setValue(value);
                        flag = true;
                    }
                    linkedList.add(kvPair);
                } catch (ArrayIndexOutOfBoundsException indexOutOfBoundsException) {
                    //ignore any incorrect input
                }
            }
            //if the value is not in the file it just appends to file
            if (!flag) {
                String outPutString = key + "," + value + ";";
                Files.write(
                        Paths.get("/" + storageLocation + "/" + filename),
                        outPutString.getBytes(),
                        StandardOpenOption.APPEND);
                return PutResult.SUCCESS;
            }
            //if the key is in the file we need to update the value
            writeOutKVPairs(linkedList);
            return PutResult.UPDATE;

        } catch (FileNotFoundException fileNotFoundException) {
            KVServer.log.warning("Error while trying to open non-existent storage file");
            return null;

        } catch (IOException e) {
            KVServer.log.warning("Error while trying to open non-existent storage file");
            throw new RuntimeException(e);
        }
    }

    private void writeOutKVPairs(LinkedList<KVPair<String, String>> linkedList) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter("/" + storageLocation + "/" + filename);
        StringBuilder stringBuilder = new StringBuilder();
        for (KVPair<String, String> stringStringKVPair : linkedList) {
            String outPutString = stringStringKVPair.getKey() + "," + stringStringKVPair.getValue() + ";";
            stringBuilder.append(outPutString);
        }
        writer.write(stringBuilder.toString());
        writer.flush();
        writer.close();
    }

    /**
     * Deletes the key-value pair associated with the provided key from the storage.
     *
     * @param key The key whose associated value is to be deleted.
     * @return The deleted key-value pair, or null if the key was not found.
     */
    public KVPair<String, String> delete(String key) {
        KVPair<String, String> outPutKvPair = null;
        try {
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            LinkedList<KVPair<String, String>> linkedList = new LinkedList<>();

            while (input.hasNext()) {
                String[] output = input.next().split(",");
                KVPair<String, String> kvPair = new KVPair<>(output[0], output[1]);

                if (kvPair.getKey().equals(key)) {
                    outPutKvPair = kvPair;
                    continue;
                }
                linkedList.add(kvPair);
            }
            //if the key is in the file we need to update the value
            writeOutKVPairs(linkedList);

            return outPutKvPair;
        } catch (FileNotFoundException fileNotFoundException) {
            KVServer.log.warning("Error while trying to open non-existent storage file");
            return null;
        }
    }

    /**
     * Initializes the persistent storage. This method ensures that the storage file exists.
     * If the file doesn't exist, it will create one.
     *
     * @throws RuntimeException If there's an error during storage file creation.
     */
    public void initializePersistentStorage() {
        try {
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + filename));
        } catch (FileNotFoundException e) {
            try {
                File dir = new File("/" + storageLocation);
                dir.mkdirs();
                File file = new File("/" + storageLocation + "/" + filename);
                file.createNewFile();
                System.out.println("CREATED FILE at:" + "/" + storageLocation + "/" + filename);
            } catch (Exception ex) {
                KVServer.log.warning("Error while trying to create storage file at start-up");
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Fetches all the data present in the storage.
     *
     * @return A string representation of all data in the storage.
     */
    public String getAllData() {
        StringBuilder stringBuilder = new StringBuilder();
        Scanner input;
        try {
            input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            while (input.hasNext()) {
                String[] output = input.next().split(",");
                try {
                    String input1 = output[0].replaceAll("\r\n", "");
                    String input2 = output[1].replaceAll("\r\n", "");
                    System.out.println(Arrays.toString(output));
                    stringBuilder.append(input1);
                    stringBuilder.append(",");
                    stringBuilder.append(input2);
                    stringBuilder.append(";");
                } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
                    //skip over invalid/malformed data
                }
            }
            input.close();
            return stringBuilder.toString();

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Saves a piece of data to the storage.
     *
     * @param data   The data to be saved.
     * @param append A flag to indicate whether to append the data or overwrite existing data.
     */
    public void saveData(String data, boolean append) {
        try {
            FileWriter fw = new FileWriter("/" + storageLocation + "/" + filename, append);
            fw.write(data);
            fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetches a range of data based on key hashes from the storage.
     *
     * @param startKeyRange     The starting key hash for the range.
     * @param keyRangeToSplitAt The ending key hash for the range.
     * @param consistentHashing An instance of the ConsistentHashing class to aid in range checks.
     * @return A string representation of the data within the specified range.
     */
    public String getDataBetweenKeyRanges(String startKeyRange, String keyRangeToSplitAt, ConsistentHashing consistentHashing) {
        StringBuilder dataToStay = new StringBuilder();
        StringBuilder dataToTransfer = new StringBuilder();
        Scanner input;
        try {
            input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            while (input.hasNext()) {
                try {
                    String[] output = input.next().split(",");
                    String key = consistentHashing.getMD5Hash(output[0]);
                    if (checkIfInRange(key, startKeyRange, keyRangeToSplitAt)) {
                        String input1 = output[0].replaceAll("\r\n", "");
                        String input2 = output[1].replaceAll("\r\n", "");

                        dataToTransfer.append(input1);
                        dataToTransfer.append(",");
                        dataToTransfer.append(input2);
                        dataToTransfer.append(";");
                    } else {
                        String input1 = output[0].replaceAll("\r\n", "");
                        String input2 = output[1].replaceAll("\r\n", "");

                        dataToStay.append(input1);
                        dataToStay.append(",");
                        dataToStay.append(input2);
                        dataToStay.append(";");
                    }
                } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
                    //skip over invalid/malformed data
                }

            }
            saveData(dataToStay.toString(), false);
            input.close();
            return dataToTransfer.toString();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param hash
     * @param startRange
     * @param endRange
     * @return
     */
    private boolean checkIfInRange(String hash, String startRange, String endRange) {
        if (startRange.compareTo(hash) < 0 && endRange.compareTo(hash) >= 0) {
            return true;
        } else if (startRange.compareTo(endRange) >= 0) {
            if (startRange.compareTo(hash) < 0 && endRange.compareTo(hash) <= 0) {
                return true;
            } else return startRange.compareTo(hash) > 0 && endRange.compareTo(hash) >= 0;
        }
        return false;
    }


    /**
     * Clears the storage file, removing all data.
     */
    public void clearFile() {
        try {
            new FileWriter("/" + storageLocation + "/" + filename, false).close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes the storage file from the system.
     *
     * @return True if the file was successfully deleted, false otherwise.
     */
    public boolean deleteFile() {
        try {
            File f = new File("/" + storageLocation + "/" + filename);
            return f.delete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

