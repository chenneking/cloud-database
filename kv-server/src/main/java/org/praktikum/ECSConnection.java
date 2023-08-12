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

    /**
     * Constructor to initialize the ECSConnection.
     *
     * @param clientSocket       The socket associated with the ECS connection.
     * @param storageUnit        The storage unit of the KVServer.
     * @param hashing            Hashing utility (although not used directly in this class).
     * @param kvServer           Reference to the main server.
     * @param ip                 IP address of the server.
     * @param port               Port number of the server.
     * @param customEndRangeHash Custom hash value specifying end range of keys.
     */
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
     * Main execution method for the thread. Listens for incoming requests
     * from the ECS and processes them.
     */
    @Override
    public void run() {
        if (customEndRangeHash != null && !customEndRangeHash.equals("")) {
            messageHandler.send("kvServer " + port + " " + ip + " " + customEndRangeHash);
        } else {
            messageHandler.send("kvServer " + port + " " + ip);
        }
        try {
            while (isOpen) {
                byte[] input = messageHandler.receive();
                if (input != null) {
                    String clientRequest = new String(input, StandardCharsets.UTF_8);
                    //System.out.println(clientRequest);
                    executeRequest(clientRequest);
                } else {
                    //the input is null if the connected server disconnected
                    close();
                }
            }
        } finally {
            storageUnit.flushCache();
        }
    }

    /**
     * Processes the incoming request from the ECS and performs the
     * appropriate action based on the command received.
     *
     * @param clientRequest String representation of the ECS command.
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
        } else {
            error();
            KVServer.log.info("Received unknown command: " + clientRequest);
        }
    }

    /**
     * Sends an error message to the connected ECS.
     */
    public void error() {
        messageHandler.send("error unknown command!");
    }


    /**
     * Sends a specific message/data to the connected ECS.
     *
     * @param data The message/data to be sent.
     */
    public void send(String data) {
        messageHandler.send(data);
    }

    /**
     * Closes the current connection and releases associated resources.
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
     * Deletes the entire data in the server's storage unit.
     */
    public synchronized void deleteStorage() {
        System.out.println("deleting from storage in KV-Server");
        storageUnit.cleanPersistentStorage();
    }

    /**
     * Retrieves all data stored in the server's storage unit.
     *
     * @return A String representation of all data.
     */
    public synchronized String getAllData() {
        return storageUnit.getAllData();
    }

    /**
     * Retrieves data that falls within a specific key range.
     *
     * @param keyRangeToSplitAt The hash value specifying the end range.
     * @return Data within the specified key range.
     */
    private synchronized String getDataBetweenKeyRanges(String keyRangeToSplitAt) {
        return storageUnit.getDataBetweenKeyRanges(kvServer.getStartRange(), keyRangeToSplitAt);
    }

    /**
     * Handles specific signals (like "TERM"). In this case,
     * it ensures the connection is closed upon receiving the signal.
     *
     * @param sig The signal received.
     */
    @Override
    public void handle(Signal sig) {
        close();
    }

    /**
     * Utility method to print out received messages, primarily for debugging purposes.
     *
     * @param string The message received.
     */
    private void received(String string) {
        System.out.println("Received: " + string);
    }
}

