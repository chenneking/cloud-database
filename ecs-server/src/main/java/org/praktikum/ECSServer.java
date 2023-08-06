package org.praktikum;

import org.praktikum.resources.RingList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ECSServer {
    public static final Logger log = Logger.getLogger("ECSServer");
    private final String address;
    private java.net.ServerSocket serverSocket;
    private final int port;
    private boolean isRunning;
    private final RingList ringList;

    public final HashMap<String, ECSCommunication> ecsCommunicationHashMap = new HashMap<>();

    public ECSServer(int port, String address, String logFilePath, Level logLevel) {
        this.port = port;
        this.isRunning = false;
        this.address = address;
        log.setLevel(logLevel);
        try {
            log.setUseParentHandlers(false);
            FileHandler fileHandler = new FileHandler(logFilePath, true);
            fileHandler.setFormatter(new SimpleFormatter());
            log.addHandler(fileHandler);
            ringList = new RingList();
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the server, executes a while loop which accepts any connecting Clients and starts a separate Thread which
     * is supposed to handle the connection to the client.
     */
    public void runServer() {
        try {
            Runtime runtime = Runtime.getRuntime();
            runtime.addShutdownHook(new Thread(this::stopServer));
            InetAddress listeningAddress = InetAddress.getByName(address);
            serverSocket = new java.net.ServerSocket(port, 100000, listeningAddress);
            isRunning = true;

            while (isRunning) {
                Socket server = serverSocket.accept();
                ECSCommunication connection = new ECSCommunication(this, server);
                new Thread(connection).start();
            }

        } catch (IOException e) {
            log.warning("Error while accepting connection from " + address + ":" + port);
        }
    }

    /**
     * Adds a new ECSCommunication to the ring list and finds its successor.
     * The method returns an `AbstractMap.SimpleEntry` object, where the key is the ECSCommunication object corresponding
     * to the successor, and the value is the start range of the hash space that the successor is responsible for.
     *
     * @param ip   The IP address of the server to be added.
     * @param port The port number of the server to be added.
     * @return An `AbstractMap.SimpleEntry` where the key is the ECSCommunication of the successor,
     * and the value is the hash range start value that the successor is responsible for.
     */

    public AbstractMap.SimpleEntry<ECSCommunication, String> addEscCommunication(String ip, String port) {
        RingList.Node node = ringList.add(ip, port).getNext();
        String keyRange = node.getStartRange();
        return new AbstractMap.SimpleEntry<>(ecsCommunicationHashMap.get(node.getIP().concat(node.getPort())), keyRange);
    }

    /**
     * removes a Node form the Ringlist and sets the succeser to the next node
     *
     * @param ip   of the to be removed node
     * @param port of the to be removed node
     * @return ECSCommunication
     */
    public ECSCommunication removeFromRing(String ip, String port) {
        RingList.Node node = ringList.remove(ip, port);
        RingList.Node successor = node.getNext();
        return ecsCommunicationHashMap.remove(successor.getIP().concat(node.getPort()));
    }

    public void updateKeyRanges(String ip, String port, String startRange, String endRange) {
        ringList.updateKeyRanges(ip, port, startRange, endRange);
    }

    /**
     * gets all meta data and converts them to a string
     *
     * @return String of meta data
     */
    public String fetchMetaData() {
        return ringList.toString();
    }

    /**
     * sends Metadata to all servers
     */
    public void sendMetaDataToAll() {
        for (ECSCommunication kvServers : ecsCommunicationHashMap.values()) {
            kvServers.sendMetaData();
        }
    }

    /**
     * Finds the previous partner connection in a hash ring architecture.
     * The 'previous' partner is the server that precedes the given server in the hash ring.
     *
     * @param address The IP address of the server for which the previous partner is to be found.
     * @param port    The port number of the server for which the previous partner is to be found.
     * @return The ECSCommunication object that corresponds to the previous partner of the provided server.
     */

    public ECSCommunication findPrevPartnerConnection(String address, String port) {
        RingList.Node node = ringList.find(ringList.getMD5Hash(address, port));
        RingList.Node successor = node.getPrev();
        System.out.println("DETERMINED SUCCESSOR OF " + address + ":" + port + " TO BE: " + successor.getIP() + ":" + successor.getPort());
        return ecsCommunicationHashMap.get(successor.getIP().concat(successor.getPort()));
    }

    /**
     * Finds the next partner connection in a hash ring architecture.
     * The 'nex' partner is the server that succeedes the given server in the hash ring.
     *
     * @param address The IP address of the server for which the succeding partner is to be found.
     * @param port    The port number of the server for which the succeding partner is to be found.
     * @return The ECSCommunication object that corresponds to the succeding partner of the provided server.
     */
    public ECSCommunication findNextPartnerConnection(String address, String port) {
        RingList.Node node = ringList.find(ringList.getMD5Hash(address, port));
        RingList.Node successor = node.getNext();
        System.out.println("DETERMINED SUCCESSOR OF " + address + ":" + port + " TO BE: " + successor.getIP() + ":" + successor.getPort());
        return ecsCommunicationHashMap.get(successor.getIP().concat(successor.getPort()));
    }

    /**
     * Stops the server and tries to close the serversocket
     */
    private void stopServer() {
        isRunning = false;
        try {
            for (ECSCommunication value : ecsCommunicationHashMap.values()) {
                value.closeConnection();
            }
            serverSocket.close();
            log.info("Stopped server");
        } catch (IOException e) {
            log.warning("Error while closing ECS socket");
        }
    }
    public static void main(String[] args) {
        ECSServer ecsServer = new ECSServer(44331, "0.0.0.0", "123",Level.ALL);
        ecsServer.runServer();
    }
}
