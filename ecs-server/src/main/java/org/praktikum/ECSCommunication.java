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

    public ECSCommunication(ECSServer ecsServer, Socket clientSocket) {
        this.messageHandler = new ECSMessageHandler(clientSocket);
        this.ecsServer = ecsServer;
        isOpen = true;
        this.clientSocket = clientSocket;
    }

    /**
     * Main method of the Thread. Sends a message to the client if connected successfully
     */
    @Override
    public void run() {
        while (isOpen) {
            byte[] input = messageHandler.receive();
            if (input != null) {
                String clientRequest = new String(input, StandardCharsets.UTF_8);
                received(ip, port, clientRequest);
                executeRequest(clientRequest);
            }
            else {
                //the input is null if the connected server disconnected
                closeConnection();
            }
        }
    }

    /**
     * Handles the addition of a new key-value server to the ECS.
     *
     * @throws RuntimeException if any communication error occurs with the new server or its predecessor.
     */

    public synchronized void addNewKVServer() {
        //recalculates the Metadata since a new StorageService connected
        AbstractMap.SimpleEntry<ECSCommunication, String> entry = ecsServer.addEscCommunication(ip, port);
        prevConnection = entry.getKey();
        //Initialize the new storage server with the updated meta
        sendMetaData();
        //write locks the successor
        prevConnection.setWriteLock();
        //sends request to the Successor to transfer the Data
        prevConnection.getDataFromKeyRange(this.ip, this.port, entry.getValue());
    }


    /**
     * Provides the input validation and delegation of tasks for the different commands
     *
     * @param serverRequest A string array of command tokens
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
                addNewKVServer();
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
        }
    }

    /**
     * Sets a write lock on the server
     */
    public synchronized void setWriteLock() {
        messageHandler.send("set_write_lock");
        sent(ip, port, "set_write_lock");
    }

    /**
     * Releses thge write lock
     */
    public synchronized void releaseWriteLock() {
        messageHandler.send("remove_write_lock");
        sent(ip, port, "remove_write_lock");
    }

    /**
     * Gets the data form the given key range from the server with the ip and port
     *
     * @param ip       from the server
     * @param port     form the server
     * @param keyRange of the data
     */
    public synchronized void getDataFromKeyRange(String ip, String port, String keyRange) {
        messageHandler.send("request_data_key_range " + ip + " " + port + " " + keyRange);
        sent(this.ip, this.port, "request_data_key_range Port: " + this.port + " Ip:" + this.ip + " KeyRange: " + keyRange);
    }

    /**
     * Sends a string via the message handler
     *
     * @param message
     */
    private void send(String message) {
        messageHandler.send(message);
        sent(ip, port, message);
    }

    /**
     * sends the metadata to the server with the ip and port
     */
    public synchronized void sendMetaData() {
        messageHandler.send("metadata " + ecsServer.fetchMetaData());
        sent(ip, port, "metadata " + ecsServer.fetchMetaData());
    }

    /**
     * Prints out the a string with the ip ,port and the given String
     *
     * @param ip     of the ECS connection
     * @param port   of the ECS connection
     * @param string to be printed
     */
    private void received(String ip, String port, String string) {
        if (ip != null && port != null) {
            System.out.println("ECS CONNECTION " + ip + ":" + port + "| Received: " + string);
        }
        else {
            System.out.println("Received: " + string);
        }
    }

    /**
     * Prints out the a string with the ip ,port and the given String
     *
     * @param ip     of the ECS connection
     * @param port   of the ECS connection
     * @param string to be printed
     */
    private void sent(String ip, String port, String string) {
        if (ip != null && port != null) {
            System.out.println("ECS CONNECTION " + ip + ":" + port + "| Sent: " + string);
        }
        else {
            System.out.println("Sent: " + string);
        }
    }

    /**
     * Closes the message handler and the socket
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
