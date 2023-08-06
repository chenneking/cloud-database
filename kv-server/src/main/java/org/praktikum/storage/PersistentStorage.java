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
    public KVPair<String, String> get(String key) {
        try {
            Scanner input = new Scanner(new File("/" + storageLocation + "/" + filename));
            input.useDelimiter(";");
            try {
                String[] output = input.next().split(",");
                KVPair<String, String> kvPair = new KVPair<>(output[0], output[1]);

                while (input.hasNext() & ! kvPair.getKey().equals(key)) {
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
            if (! flag) {
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

    //has to create a data file if it does not exist already
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

    public void saveData(String data, boolean append) {
        try {
            FileWriter fw = new FileWriter("/" + storageLocation + "/" + filename, append);
            fw.write(data);
            fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
                    }
                    else {
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
        }
        else if (startRange.compareTo(endRange) >= 0) {
            if (startRange.compareTo(hash) < 0 && endRange.compareTo(hash) <= 0) {
                return true;
            }
            else return startRange.compareTo(hash) > 0 && endRange.compareTo(hash) >= 0;
        }
        return false;
    }

    //adds a method to clear the file for testing purposes

    /**
     * Clears a file from the given sotrage location and name
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
     * Deletes the file given in the storage location
     *
     * @return True if the file is deleted
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

