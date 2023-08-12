package org.praktikum.storage;

import org.praktikum.resources.PutResult;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class StorageThread implements Runnable {

    private final String command;
    private final String key;
    private String value;
    private PutResult putResult = null;

    private final String storageLocation;
    private final String FILENAME;

    private final StringBuilder outPut = new StringBuilder();
    private String completeData = null;

    /**
     * Constructs a new StorageThread instance.
     *
     * @param command         The storage operation command: "get", "put", "delete", etc.
     * @param key             The key to be operated on.
     * @param value           The value associated with the key (only for put operations).
     * @param storageLocation The directory location of the storage file.
     * @param FILENAME        The name of the storage file.
     */
    public StorageThread(String command, String key, String value, String storageLocation, String FILENAME) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.storageLocation = storageLocation;
        this.FILENAME = FILENAME;
    }

    @Override
    public void run() {
        executeCommand(command);
    }

    /**
     * Writes the contents of `outPut` into the storage file.
     *
     * @throws IOException If there's an error writing to the file.
     */
    private void writeFile() {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/" + storageLocation + "/" + FILENAME));
            out.write(outPut.toString());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieves the value associated with the given key from the storage.
     *
     * @throws FileNotFoundException If the storage file is not found.
     * @throws IOException           If there's an error reading the file.
     */
    private synchronized void get() {
        try {
            final BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("/" + storageLocation + "/" + FILENAME), StandardCharsets.UTF_8));
            String start;
            while ((start = input.readLine()) != null) {
                String[] temp = start.split(",");
                if (temp[0].equals(key)) {
                    value = temp[1];
                    break;
                }
            }
            input.close();

        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stores or updates the value associated with the given key in the storage.
     *
     * @throws FileNotFoundException If the storage file is not found.
     * @throws IOException           If there's an error reading or writing to the file.
     */
    private synchronized void put() {
        try {
            final BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("/" + storageLocation + "/" + FILENAME), StandardCharsets.UTF_8));
            String start;
            while ((start = input.readLine()) != null) {
                String[] temp = start.split(",");
                if (temp[0].equals(key)) {
                    putResult = PutResult.UPDATE;
                    outPut.append(temp[0]);
                    outPut.append(",");
                    outPut.append(value);
                    outPut.append("\n");
                } else {
                    outPut.append(temp[0]);
                    outPut.append(",");
                    outPut.append(value);
                    outPut.append(temp[1]);
                    outPut.append("\n");
                }
            }
            if (putResult == null) {
                putResult = PutResult.SUCCESS;
                outPut.append(key);
                outPut.append(",");
                outPut.append(value);
                outPut.append("\n");
            }

            writeFile();
            input.close();

        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes the key-value pair associated with the given key from the storage.
     *
     * @throws FileNotFoundException If the storage file is not found.
     * @throws IOException           If there's an error reading or writing to the file.
     */
    private synchronized void delete() {
        try {
            final BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("/" + storageLocation + "/" + FILENAME), StandardCharsets.UTF_8));
            String start;
            while ((start = input.readLine()) != null) {
                String[] temp = start.split(",");
                if (temp[0].equals(key)) {
                    value = temp[1];
                } else {
                    outPut.append(temp[0]);
                    outPut.append(",");
                    outPut.append(value);
                    outPut.append(temp[1]);
                    outPut.append("\n");
                }
            }
            writeFile();
            input.close();

        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetches all data from the storage.
     *
     * @throws FileNotFoundException If the storage file is not found.
     * @throws IOException           If there's an error reading the file.
     */
    private synchronized void getAllData() {
        final BufferedReader input;
        try {
            input = new BufferedReader(new InputStreamReader(new FileInputStream("/" + storageLocation + "/" + FILENAME), StandardCharsets.UTF_8));
            String start;
            StringBuilder stringBuilder = new StringBuilder();
            while ((start = input.readLine()) != null) {
                stringBuilder.append(start);
                stringBuilder.append("\n");
            }
            completeData = stringBuilder.toString();
            //specialCase if file is empty
            if (completeData.equals("null")) {
                completeData = "";
            }

        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * Executes the given storage operation command.
     *
     * @param command The operation command: "get", "put", "delete", "getAll".
     */
    private void executeCommand(String command) {
        switch (command) {
            case "put" -> put();
            case "get" -> get();
            case "getAll" -> getAllData();
            //TODO: the default delete here doesn't seem right
            default -> delete();
        }
    }

    public String getCompleteData() {
        return completeData;
    }

    public String getCommand() {
        return command;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public PutResult getPutResult() {
        return putResult;
    }

}
