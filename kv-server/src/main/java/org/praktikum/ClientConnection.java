package org.praktikum;

import org.praktikum.communication.MessageHandler;
import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.PutResult;
import org.praktikum.resources.RingList;
import org.praktikum.resources.UsageMetrics;
import org.praktikum.storage.KVStore;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;


public class ClientConnection implements Runnable {
    private boolean isOpen;
    private final KVStore storageUnit;
    private final MessageHandler messageHandler;
    private final ConsistentHashing hashing;
    private final KVServer kvServer;
    private final Socket clientSocket;
    private final static int OPERATION_COUNT_OFFLOAD_THRESHOLD = 3000;

    public ClientConnection(Socket clientSocket, ConsistentHashing hashing, KVServer kvServer) {
        this.storageUnit = kvServer.getStore();
        this.messageHandler = new MessageHandler(clientSocket);
        isOpen = true;
        this.hashing = hashing;
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
    }


    /**
     * Main method of the Thread. Sends a message to the client if connected successfully
     */
    @Override
    public void run() {
        messageHandler.send("Connected successfully!");
        while (isOpen) {
            byte[] input = messageHandler.receive();
            if (input != null) {
                String clientRequest = new String(input, StandardCharsets.UTF_8);
                executeRequest(clientRequest);
            }
            else {
                //the input is null if the connected server disconnected
                close();
            }
        }
    }

    /**
     * Executes the correct request given by the user by splitting the request into different tokens and executing the
     * correct function
     *
     * @param clientRequest String as the client the input
     */
    private void executeRequest(String clientRequest) {
        String[] tokens = clientRequest.trim().split("\\s+");
        if (tokens.length == 0) {
            return;
        }
        switch (tokens[0]) {
            case "put" -> {
                if (tokens.length < 2) {
                    error();
                    break;
                }
                StringBuilder builder = new StringBuilder();
                int tokenLength = tokens.length;
                for (int i = 2; i < tokenLength; i++) {
                    builder.append(tokens[i]);
                    if (i != tokenLength - 1) {
                        builder.append(" ");
                    }
                }
                put(tokens[1], builder.toString());
                try {
                    if(kvServer.getUsageMetrics().getOperationsLast30s() > OPERATION_COUNT_OFFLOAD_THRESHOLD){
                        offloadKeys();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case "get" -> {
                if (tokens.length < 2) {
                    error();
                    break;
                }
                get(tokens[1]);
                try {
                    if(kvServer.getUsageMetrics().getOperationsLast30s() > OPERATION_COUNT_OFFLOAD_THRESHOLD){
                        offloadKeys();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case "delete" -> {
                if (tokens.length < 2 || tokens.length > 3) {
                    error();
                    break;
                }
                delete(tokens[1]);
                try {
                    if(kvServer.getUsageMetrics().getOperationsLast30s() > OPERATION_COUNT_OFFLOAD_THRESHOLD){
                        offloadKeys();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case "save_data" -> {
                String[] dataToSend = Arrays.copyOfRange(tokens, 1, tokens.length);
                String data = String.join(" ", dataToSend);
                System.out.println("Received: save_data " + data);
                storageUnit.saveData(data, true);
                close();
            }
            case "save_data_buckets" -> {
                String[] dataToSend = Arrays.copyOfRange(tokens, 1, tokens.length);
                String data = String.join(" ", dataToSend);
                System.out.println("Received: save_data " + data);
                storageUnit.saveData(data, true);
                kvServer.getFrequencyTable().addDummyBucket(data);
            }
            case "ECS" -> {
                if (tokens[1].equals("ping_request"))
                    messageHandler.send("server_is_running");
            }
            case "keyrange" -> sendKeyRange();
            case "server_put" -> {
                StringBuilder builder = new StringBuilder();
                int tokenLength = tokens.length;
                for (int i = 4; i < tokenLength; i++) {
                    builder.append(tokens[i]);
                    if (i != tokenLength - 1) {
                        builder.append(" ");
                    }
                }
                System.out.println("Received: server_put " + tokens[1] + " " + tokens[2] + " " + tokens[3] + " " + builder);
                serverPut(tokens[1], tokens[2], tokens[3], builder.toString());
                close();
            }
            case "server_delete" -> {
                System.out.println("server_delete " + tokens[1] + " " + tokens[2] + " " + tokens[3]);
                serverDelete(tokens[1], tokens[2], tokens[3]);
                close();
            }
            case "keyrange_read" -> sendKeyRangeRead();
            case "closing_client" -> close();
            case "request_replica_data" -> {
                String allData = storageUnit.getAllData();
                messageHandler.send(allData);
                System.out.println("Sent (replication data): " + allData);
            }
            case "replica_data_update" -> {
                StringBuilder builder = new StringBuilder();
                int tokenLength = tokens.length;
                for (int i = 3; i < tokenLength; i++) {
                    builder.append(tokens[i]);
                    if (i != tokenLength - 1) {
                        builder.append(" ");
                    }
                }
                String data = builder.toString();

                System.out.println(new Date().getTime() + " Received: " + tokens[0] + " " + tokens[1] + " " + tokens[2] + " " + data);

                //find corresponding replica store and save the data with append = false (i.e OVERWRITE the old data)
                String serverIPAndPort = tokens[1] + ":" + tokens[2];
                KVStore replicaStore = kvServer.getReplicaStores().get(serverIPAndPort);
                if (replicaStore != null) {
                    replicaStore.saveData(data, false);
                }
            }
            case "get_frequency_table" -> {
                messageHandler.send("\n"+kvServer.getFrequencyTable().toString());
            }
            case "get_usage_metrics_info" -> {
                messageHandler.send(kvServer.getUsageMetrics().info());
            }
            case "get_usage_metrics" -> {
                messageHandler.send(kvServer.getUsageMetrics().toString());
            }
            case "set_write_lock" -> kvServer.setWriteLock(true);
            case "remove_write_lock" -> kvServer.setWriteLock(false);
            default -> {
                error();
                KVServer.log.info("Received unknown command: " + clientRequest);
            }
        }
    }
    /**
     * Sends error message to the client
     */
    public void error() {
        messageHandler.send("error unknown command!");
    }

    /**
     * sends data via the message handler
     *
     * @param data
     */
    public void send(String data) {
        messageHandler.send(data);
    }

    /**
     * Closes message handler and client socket
     */
    public void close() {
        isOpen = false;
        messageHandler.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Puts to
     *
     * @param ip
     * @param port
     * @param key
     * @param value
     */
    private synchronized void serverPut(String ip, String port, String key, String value) {
        Map<String, KVStore> map = kvServer.getReplicaStores();
        KVStore kvStore = map.get(ip + ":" + port);
        kvStore.put(key, value);
    }

    private synchronized void serverDelete(String ip, String port, String key) {
        Map<String, KVStore> map = kvServer.getReplicaStores();
        KVStore kvStore = map.get(ip + ":" + port);
        kvStore.delete(key);
    }


    /**
     * Stores or updates a key-value pair in the server storage unit.
     * <p>
     * <p>
     * If more than 2 Nodes are in the ringlist the put operation is replicated to the next two nodes in the ringlist.
     *
     * @param key   The key associated with the value to be stored.
     * @param value The data to be stored.
     * @throws RuntimeException If an I/O error occurs when sending data to the next nodes.
     */

    private synchronized void put(String key, String value) {
        if (kvServer.isWriteLock()) {
            KVServer.log.info("Server is write-locked");
            messageHandler.send("server_write_lock");
            return;
        }
        if (kvServer.isStopped()) {
            KVServer.log.info("Server is write-locked");
            messageHandler.send("server_stopped");
            return;
        }

        String hash = hashing.getMD5Hash(key);
        boolean isRightServer = checkIfRightServer(hash);
        if (!isRightServer) {
            messageHandler.send("server_not_responsible");
            return;
        }


        PutResult status = storageUnit.put(key, value);
        if (status == PutResult.SUCCESS) {
            KVServer.log.info("Successful PUT: " + key + ":" + value);
            messageHandler.send("put_success " + key);

            kvServer.getUsageMetrics().addOperation();
            kvServer.getFrequencyTable().addToTable(key, hashing.getMD5Hash(key));

        } else if (status == PutResult.UPDATE) {
            KVServer.log.info("Successful UPDATE: " + key + ":" + value);
            messageHandler.send("put_update " + key);

            kvServer.getUsageMetrics().addOperation();
            kvServer.getFrequencyTable().addToTable(key, hashing.getMD5Hash(key));

        } else {
            KVServer.log.info("Error during PUT: " + key + ":" + value);
            messageHandler.send("put_error");
        }

        try {
            if (kvServer.getRingList().getSize() > 2) {
                RingList.Node node = kvServer.getRingList().findByIPandPort(kvServer.getAddress(), Integer.toString(kvServer.getPort()));
                RingList.Node nodeNext = node.getNext();
                RingList.Node nodeNextNext = node.getNext().getNext();
                Socket socketNext = new Socket(nodeNext.getIP(), Integer.parseInt(nodeNext.getPort()));
                Socket socketNextNext = new Socket(nodeNextNext.getIP(), Integer.parseInt(nodeNextNext.getPort()));
                MessageHandler messageHandlerNext = new MessageHandler(socketNext);
                MessageHandler messageHandlerNextNext = new MessageHandler(socketNextNext);

                System.out.println("Sent (to " + nodeNext.getIP() + ":" + nodeNext.getPort() + "): server_put " + key + " " + value);
                messageHandlerNext.send("server_put " + kvServer.getAddress() + " " + kvServer.getPort() + " " + key + " " + value);

                System.out.println("Sent (to " + nodeNextNext.getIP() + ":" + nodeNextNext.getPort() + "): server_put " + key + " " + value);
                messageHandlerNextNext.send("server_put " + kvServer.getAddress() + " " + kvServer.getPort() + " " + key + " " + value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    private synchronized void offloadKeys() throws IOException {
        RingList.Node node = kvServer.getRingList().findByIPandPort(kvServer.getAddress(), Integer.toString(kvServer.getPort()));
        RingList.Node nodeNext = node.getNext();
        RingList.Node nodePrev = node.getPrev();
        Socket socketNext = new Socket(nodeNext.getIP(), Integer.parseInt(nodeNext.getPort()));
        Socket socketPrev = new Socket(nodePrev.getIP(), Integer.parseInt(nodePrev.getPort()));
        MessageHandler messageHandlerNext = new MessageHandler(socketNext);
        MessageHandler messageHandlerPrev = new MessageHandler(socketPrev);

        messageHandlerNext.send("get_usage_metrics_info");
        //skips over the welcome message
        messageHandlerNext.receive();
        int nextLoad = Integer.parseInt(new String(messageHandlerNext.receive(), StandardCharsets.UTF_8));
        messageHandlerPrev.send("get_usage_metrics_info");
        messageHandlerPrev.receive();
        int prevLoad = Integer.parseInt(new String(messageHandlerPrev.receive(), StandardCharsets.UTF_8));
        System.out.println("-----next load: " + nextLoad);
        System.out.println("-----prev Load: " + prevLoad);
        System.out.println("-----current Server load: " + kvServer.getUsageMetrics().toString());

        String [] keyRange = kvServer.getFrequencyTable().calculateOffloadKeyRange(true);
        String data = kvServer.getStore().getDataBetweenKeyRanges(keyRange[0], keyRange[1]);
        //if the load of the current Server is smaller than the load of the other 2 servers you do nothing
        if(kvServer.getUsageMetrics().getOperationsLast30s() < nextLoad && kvServer.getUsageMetrics().getOperationsLast30s() < prevLoad){
            return;
        }
        //if the load of the next server is smaller than the load of prev you offload your keys to the next server
        if(nextLoad < prevLoad){
            messageHandlerNext.send("set_write_lock");
            kvServer.setWriteLock(true);
            messageHandlerNext.send("save_data_buckets" + data);
            messageHandlerNext.send("release_write_lock");
            kvServer.setWriteLock(false);
            changeKeyRangeRequest(keyRange[0], keyRange[1]);
        }
        else{
            messageHandlerPrev.send("set_write_lock");
            kvServer.setWriteLock(true);
            messageHandlerPrev.send("save_data_buckets" + data);
            messageHandlerPrev.send("release_write_lock");
            kvServer.setWriteLock(false);
            changeKeyRangeRequest(keyRange[0], keyRange[1]);
        }
        messageHandlerNext.close();
        messageHandlerPrev.close();
        socketNext.close();
        socketPrev.close();
    }
    private synchronized void changeKeyRangeRequest(String startRange, String endRang){
        kvServer.getEcsConnection().send("update_keyrange "+ startRange + " " + endRang);
    }

    /**
     * Calls the get method from the kvStore and sends the according message to the Client
     *
     * @param key String <key>
     */
    private synchronized void get(String key) {
        if (kvServer.isStopped()) {
            KVServer.log.info("Server is write-locked");
            messageHandler.send("server_stopped");
            return;
        }
        String hash = hashing.getMD5Hash(key);
        String bucketIPAndPort = checkIfRightServerGet(hash);
        System.out.println("Determined " + key + " to be from server: " + bucketIPAndPort);
        if (bucketIPAndPort == null) {
            messageHandler.send("server_not_responsible");
            return;
        }
        String value;
        if (bucketIPAndPort.equals(kvServer.getAddress() + ":" + kvServer.getPort())) {
            value = storageUnit.get(key);
        }
        else {
            value = kvServer.getReplicaStores().get(bucketIPAndPort).get(key);
        }

        if (value == null) {
            KVServer.log.info("Error during GET: " + key);
            messageHandler.send("get_error " + key);
        }
        else {
            kvServer.getUsageMetrics().addOperation();
            kvServer.getFrequencyTable().addToTable(key, hashing.getMD5Hash(key));
            KVServer.log.info("Successful GET: " + key + ":" + value);
            messageHandler.send("get_success " + key + " " + value);
        }

    }

    /**
     * Calls the delete method from the kvStore and sends the according message to the Client
     *
     * @param key String <key>
     */
    private synchronized void delete(String key) {
        if (kvServer.isWriteLock()) {
            KVServer.log.info("Server is write-locked");
            messageHandler.send("server_write_lock");
            return;
        }
        if (kvServer.isStopped()) {
            KVServer.log.info("Server is write-locked");
            messageHandler.send("server_stopped");
            return;
        }
        String hash = hashing.getMD5Hash(key);
        boolean isRightServer = checkIfRightServer(hash);
        if (! isRightServer) {
            messageHandler.send("server_not_responsible");
            return;
        }
        String value = storageUnit.delete(key);
        if (value != null) {
            KVServer.log.info("Successful DELETE: " + key + ":" + value);
            messageHandler.send("delete_success " + key + " " + value);
            kvServer.getUsageMetrics().addOperation();
            kvServer.getFrequencyTable().deleteFromTable(key, hashing.getMD5Hash(key));
        }
        else {
            KVServer.log.info("Error during DELETE: " + key);
            messageHandler.send("delete_error " + key);
        }

        try {
            if (kvServer.getRingList().getSize() > 2) {
                RingList.Node node = kvServer.getRingList().findByIPandPort(kvServer.getAddress(), Integer.toString(kvServer.getPort()));
                Socket socketNext = new Socket(node.getNext().getIP(), Integer.parseInt(node.getNext().getPort()));
                Socket socketNextNext = new Socket(node.getNext().getNext().getIP(), Integer.parseInt(node.getNext().getNext().getPort()));
                MessageHandler messageHandlerNext = new MessageHandler(socketNext);
                MessageHandler messageHandlerNextNext = new MessageHandler(socketNextNext);

                System.out.println("Sent: server_delete " + key);
                messageHandlerNext.send("server_delete " + kvServer.getAddress() + " " + kvServer.getPort() + " " + key);
                System.out.println("Sent: server_delete " + key);
                messageHandlerNextNext.send("server_delete " + kvServer.getAddress() + " " + kvServer.getPort() + " " + key);

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sends key range via the message handler.
     */
    private synchronized void sendKeyRange() {
        messageHandler.send("keyrange_success " + kvServer.getRingList());
    }

    /**
     * Sends key range read via the message handler.
     */
    private synchronized void sendKeyRangeRead() {
        messageHandler.send("keyrange_read_success " + kvServer.getRingList().getKeyRangeRead());
    }

    /**
     * Checks if it's the right server for the given hash
     *
     * @param hash
     * @return returns true if has is in the range of the current server.
     */
    private boolean checkIfRightServer(String hash) {
        if (kvServer.getStartRange().compareTo(hash) < 0 && kvServer.getEndRange().compareTo(hash) >= 0) {
            return true;
        }
        else if (kvServer.getStartRange().compareTo(kvServer.getEndRange()) >= 0) {
            if (kvServer.getStartRange().compareTo(hash) < 0 && kvServer.getEndRange().compareTo(hash) <= 0) {
                return true;
            }
            else return kvServer.getStartRange().compareTo(hash) > 0 && kvServer.getEndRange().compareTo(hash) >= 0;
        }
        return false;
    }

    /**
     * Checks if the current server is the right server in replication or not for the providend hash key
     *
     * @param hash
     * @return Correct servers IP and Port
     */
    private String checkIfRightServerGet(String hash) {
        String serverIPAndPort = kvServer.getAddress() + ":" + kvServer.getPort();
        if (kvServer.getStartRange().compareTo(hash) < 0 && kvServer.getEndRange().compareTo(hash) >= 0) {
            return serverIPAndPort;
        }
        else if (kvServer.getStartRange().compareTo(kvServer.getEndRange()) >= 0) {
            if (kvServer.getStartRange().compareTo(hash) < 0 && kvServer.getEndRange().compareTo(hash) <= 0) {
                return serverIPAndPort;
            }
            else if (kvServer.getStartRange().compareTo(hash) > 0 && kvServer.getEndRange().compareTo(hash) >= 0) {
                return serverIPAndPort;
            }
        }

        if (kvServer.getRingList().getSize() > 2) {
            for (String key : kvServer.getReplicaStores().keySet()) {
                String[] split = key.split(":");
                RingList.Node node = kvServer.getRingList().findByIPandPort(split[0], split[1]);
                String replicaIPAndPort = node.getIP() + ":" + node.getPort();
                if (node.getStartRange().compareTo(hash) < 0 && node.getEndRange().compareTo(hash) >= 0) {
                    return replicaIPAndPort;
                }
                else if (node.getStartRange().compareTo(node.getEndRange()) >= 0) {
                    if (node.getStartRange().compareTo(hash) < 0 && node.getEndRange().compareTo(hash) <= 0) {
                        return replicaIPAndPort;
                    }
                    else if (node.getStartRange().compareTo(hash) > 0 && node.getEndRange().compareTo(hash) >= 0) {
                        return replicaIPAndPort;
                    }
                }

            }
        }
        return null;
    }

}
