package org.praktikum;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;

public class ECSCommunication implements Runnable {
    private boolean isOpen;
    private final ECSMessageHandler messageHandler;
    private final ECSServer ecsServer;
    private String ip;
    private String port;
    private final Socket clientSocket;
    private ECSCommunication prevConnection;

    /**
     * Constructor for the ECSCommunication class.
     * Initializes the message handler with the provided client socket and sets the associated ECSServer.
     *
     * @param ecsServer    The associated ECSServer instance.
     * @param clientSocket The client socket for communication.
     */
    public ECSCommunication(ECSServer ecsServer, Socket clientSocket) {
        this.messageHandler = new ECSMessageHandler(clientSocket);
        this.ecsServer = ecsServer;
        isOpen = true;
        this.clientSocket = clientSocket;
    }


    /**
     * The main execution method for the ECSCommunication thread.
     * It continuously listens for incoming messages and processes them until the connection is closed.
     */
    @Override
    public void run() {
        while (isOpen) {
            byte[] input = messageHandler.receive();
            if (input != null) {
                String clientRequest = new String(input, StandardCharsets.UTF_8);
                received(ip, port, clientRequest);
                executeRequest(clientRequest);
            } else {
                //the input is null if the connected server disconnected
                closeConnection();
            }
        }
    }

    /**
     * Adds a new Key-Value server to the ECS and recalculates metadata.
     * Sends the updated metadata to the new server and triggers data transfer from the predecessor server if needed.
     *
     * @param hashString Custom end range hash string provided during server startup.
     */
    public synchronized void addNewKVServer(String hashString) {
        //recalculates the Metadata since a new StorageService connected
        AbstractMap.SimpleEntry<ECSCommunication, String> entry = ecsServer.addEscCommunication(ip, port, hashString);
        prevConnection = entry.getKey();
        //Initialize the new storage server with the updated meta
        sendMetaData();
        //write locks the successor
        prevConnection.setWriteLock();
        //sends request to the Successor to transfer the Data
        prevConnection.getDataFromKeyRange(this.ip, this.port, entry.getValue());
    }


    /**
     * Parses and executes the provided request from the server.
     * Based on the command, different actions are taken, such as adding a new KV server or updating metadata.
     *
     * @param serverRequest The raw request string from the server.
     */
    public synchronized void executeRequest(String serverRequest) {
        String[] tokens = serverRequest.trim().split("\\s+");
        if (tokens.length == 0) {
            return;
        }
        switch (tokens[0]) {
            case "kvServer" -> {
                port = tokens[1];
                ip = tokens[2];
                // If the KVServer provides a customized endrange that it would like, pass it on to the startup logic, keep it null otherwise.
                String customHashString = null;
                if (tokens.length == 4) {
                    customHashString = tokens[3];
                }
                ecsServer.ecsCommunicationHashMap.put(ip.concat(port), this);
                //finds the right successor for every request, as the connection isn't notified about updates and the successor might be outdated.
                //successor = ecsServer.findSuccessorConnection(ip, port);
                try {
                    PingCommunication pingCommunication = new PingCommunication(new Socket(ip, Integer.parseInt(port)), ip, port);
                    new Thread(pingCommunication).start();

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                messageHandler.send("connection_ecs_established");
                sent(ip, port, "connection_ecs_established");
                addNewKVServer(customHashString);
            }
            //KVServer tries to close
            case "close" -> {
                //finds the right successor for every request, as the connection isn't notified about updates and the successor might be outdated.
                prevConnection = ecsServer.findPrevPartnerConnection(ip, port);
                ecsServer.removeFromRing(ip, port);
                setWriteLock();
                prevConnection.sendMetaData();
            }
            //Signals the ECS that all the data from the keyRange is sent
            case "data_key_range_sent" -> {
                //finds the left predecessor for every request, as the connection isn't notified about updates and the successor might be outdated.
                ECSCommunication nextConnection = ecsServer.findNextPartnerConnection(ip, port);
                ecsServer.sendMetaDataToAll();
                nextConnection.releaseWriteLock();

            }
            case "data_from_key_range" -> {
                prevConnection = ecsServer.findPrevPartnerConnection(ip, port);
                prevConnection.send("data_received");
            }
            //Signals the ECS that all the data is sent
            case "data_complete_send" -> {
                closeConnection();
                ecsServer.sendMetaDataToAll();
            }
            // CMD: update_keyrange start_range end_range
            case "update_keyrange" -> {
                String newStartRange = tokens[1];
                String newEndRange = tokens[2];
                ecsServer.updateKeyRanges(ip, port, newStartRange, newEndRange);
                ecsServer.sendMetaDataToAll();
            }
        }
    }

    /**
     * Sends a write lock command to the server to prevent it from handling write operations.
     */
    public synchronized void setWriteLock() {
        messageHandler.send("set_write_lock");
        sent(ip, port, "set_write_lock");
    }


    /**
     * Sends a command to the server to release any previously set write locks.
     */
    public synchronized void releaseWriteLock() {
        messageHandler.send("remove_write_lock");
        sent(ip, port, "remove_write_lock");
    }

    /**
     * Requests a given server to send data for a specific hash range.
     * This is typically used during data migration between servers.
     *
     * @param ip       IP address of the server to request data from.
     * @param port     Port number of the server to request data from.
     * @param keyRange Hash range of the data to be requested.
     */
    public synchronized void getDataFromKeyRange(String ip, String port, String keyRange) {
        messageHandler.send("request_data_key_range " + ip + " " + port + " " + keyRange);
        sent(this.ip, this.port, "request_data_key_range Port: " + port + " Ip:" + ip + " KeyRange: " + keyRange);
    }

    /**
     * Sends a message to a client via the message handler.
     * Additionally, logs the sent message with associated IP and port.
     *
     * @param message The message to be sent.
     */
    private void send(String message) {
        messageHandler.send(message);
        sent(ip, port, message);
    }


    /**
     * Sends metadata to the client server identified by the current IP and port.
     * Logs the sent metadata for debugging and tracking purposes.
     */
    public synchronized void sendMetaData() {
        messageHandler.send("metadata " + ecsServer.fetchMetaData());
        sent(ip, port, "metadata " + ecsServer.fetchMetaData());
    }

    /**
     * Logs a received message, indicating which client it came from.
     * If IP or port are null, it logs a generic received message.
     *
     * @param ip     The IP address from which the message was received.
     * @param port   The port from which the message was received.
     * @param string The received message.
     */
    private void received(String ip, String port, String string) {
        if (ip != null && port != null) {
            System.out.println("ECS CONNECTION " + ip + ":" + port + "| Received: " + string);
        } else {
            System.out.println("Received: " + string);
        }
    }

    /**
     * Logs a sent message, indicating to which client it was sent.
     * If IP or port are null, it logs a generic sent message.
     *
     * @param ip     The IP address to which the message was sent.
     * @param port   The port to which the message was sent.
     * @param string The sent message.
     */

    private void sent(String ip, String port, String string) {
        if (ip != null && port != null) {
            System.out.println("ECS CONNECTION " + ip + ":" + port + "| Sent: " + string);
        } else {
            System.out.println("Sent: " + string);
        }
    }

    /**
     * Closes the current connection.
     * It stops the message handler, closes the client socket, and updates the connection status.
     */
    public void closeConnection() {
        isOpen = false;
        messageHandler.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
