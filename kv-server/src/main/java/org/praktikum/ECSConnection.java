package org.praktikum;

import org.praktikum.communication.MessageHandler;
import org.praktikum.resources.ConsistentHashing;
import org.praktikum.storage.KVStore;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;


public class ECSConnection implements Runnable, SignalHandler {
    private boolean isOpen;
    private final KVStore storageUnit;
    private final MessageHandler messageHandler;

    private final KVServer kvServer;
    private final String ip;
    private final String port;
    private final Socket socket;
    private String customEndRangeHash;

    public ECSConnection(Socket clientSocket, KVStore storageUnit, ConsistentHashing hashing, KVServer kvServer, String ip, int port, String customEndRangeHash) {
        this.storageUnit = storageUnit;
        this.messageHandler = new MessageHandler(clientSocket);
        isOpen = true;
        this.kvServer = kvServer;
        this.ip = ip;
        this.port = Integer.toString(port);
        this.socket = clientSocket;
        this.customEndRangeHash = customEndRangeHash;

        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, this);
    }


    /**
     * Main method of the Thread. Sends a message to the client if connected successfully
     */
    @Override
    public void run() {
        if (customEndRangeHash != null && ! customEndRangeHash.equals("")) {
            messageHandler.send("kvServer " + port + " " + ip + " " + customEndRangeHash);
        }
        else {
            messageHandler.send("kvServer " + port + " " + ip);
        }
        try {
            while (isOpen) {
                byte[] input = messageHandler.receive();
                if (input != null) {
                    String clientRequest = new String(input, StandardCharsets.UTF_8);
                    //System.out.println(clientRequest);
                    executeRequest(clientRequest);
                }
                else {
                    //the input is null if the connected server disconnected
                    close();
                }
            }
        } finally {
            storageUnit.flushCache();
        }
    }

    /**
     * Executes the correct request given by the user by splitting the request into different tokens and executing the
     * correct function
     *
     * @param clientRequest String as the client the input
     */
    private void executeRequest(String clientRequest) {
        received(clientRequest);
        String[] tokens = clientRequest.trim().split("\\s+");
        if (tokens.length == 0) {
            return;
        }
        if ("ECS".equals(tokens[0])) {
            switch (tokens[1]) {
                case "set_write_lock" -> kvServer.setWriteLock(true);
                case "remove_write_lock" -> kvServer.setWriteLock(false);
                case "metadata" -> {
                    kvServer.passNewMetaData(tokens[2]);
                    kvServer.fetchKeyRangesFromMetaData();
                }
                case "connection_ecs_established" -> kvServer.setStopped(false);
                case "save_data" -> {
                    String[] dataToSend = Arrays.copyOfRange(tokens, 2, tokens.length);
                    String data = String.join(" ", dataToSend);
                    storageUnit.saveData(data, true);
                }
                case "request_data_key_range" -> {
                    String nextIP = tokens[2];
                    int nextPort = Integer.parseInt(tokens[3]);
                    System.out.println(tokens[4]);
                    String data = getDataBetweenKeyRanges(tokens[4]);
                    try {
                        Socket socket = new Socket(nextIP, nextPort);
                        MessageHandler serverCommunication = new MessageHandler(socket);
                        System.out.println("Sent to port: " + nextPort + " and ip: " + nextIP);
                        serverCommunication.send("save_data " + data);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    messageHandler.send("data_from_key_range");
                }
                case "data_received" -> messageHandler.send("data_key_range_sent");
            }
        }
        else {
            error();
            KVServer.log.info("Received unknown command: " + clientRequest);
        }
    }

    /**
     * Sends error message to the client
     */
    public void error() {
        messageHandler.send("error unknown command!");
    }

    //waits for the inserted message

    /**
     * sends data via the message handler
     *
     * @param data
     */
    public void send(String data) {
        messageHandler.send(data);
    }

    /**
     * Closes message handler and socket
     */
    public void close() {
        isOpen = false;
        messageHandler.close();
        try {
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes one Storage Unit
     */
    public synchronized void deleteStorage() {
        System.out.println("deleting from storage in KV-Server");
        storageUnit.cleanPersistentStorage();
    }

    public synchronized String getAllData() {
        return storageUnit.getAllData();
    }

    /**
     * Retrives data form the storage unit that falls within the key range
     *
     * @param keyRangeToSplitAt
     * @return String with the data between the key range
     */
    private synchronized String getDataBetweenKeyRanges(String keyRangeToSplitAt) {
        return storageUnit.getDataBetweenKeyRanges(kvServer.getStartRange(), keyRangeToSplitAt);
    }

    @Override
    public void handle(Signal sig) {
        close();
    }

    private void received(String string) {
        System.out.println("Received: " + string);
    }
}

