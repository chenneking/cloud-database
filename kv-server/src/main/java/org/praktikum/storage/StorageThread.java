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
     * Writes the contents of `outPut` into a file in the specified storage location.
     * <p>
     * Creates a new file or overwrites an existing file at the given storage location with the specified filename. The content of `outPut` is written into the file.
     *
     * @throws IOException If an I/O error occurs while writing to the file.
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
     * Retrieves the value associated with the given key from a file stored in the specified storage location.
     *
     * @throws FileNotFoundException If the file is not found at the specified storage location.
     * @throws IOException           If an I/O error occurs while reading from the file.
     * @throws RuntimeException      If an IOException is caught.
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
     * Writes or updates the value associated with the given key from a file stored in the specified storage location.
     *
     * @throws FileNotFoundException If the file is not found at the specified storage location.
     * @throws IOException           If an I/O error occurs while reading from the file.
     * @throws RuntimeException      If an IOException is caught.
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
                }
                else {
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
     * Deletes the value associated with the given key from a file stored in the specified storage location.
     *
     * @throws FileNotFoundException If the file is not found at the specified storage location.
     * @throws IOException           If an I/O error occurs while reading from the file.
     * @throws RuntimeException      If an IOException is caught.
     */
    private synchronized void delete() {
        try {
            final BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("/" + storageLocation + "/" + FILENAME), StandardCharsets.UTF_8));
            String start;
            while ((start = input.readLine()) != null) {
                String[] temp = start.split(",");
                if (temp[0].equals(key)) {
                    value = temp[1];
                }
                else {
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
     * Retrieves all data from a file stored in the specified storage location.
     *
     * @throws FileNotFoundException If the file is not found at the specified storage location.
     * @throws IOException           If an I/O error occurs while reading from the file.
     * @throws RuntimeException      If an IOException is caught.
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
     * Executes the given command
     *
     * @param command
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
