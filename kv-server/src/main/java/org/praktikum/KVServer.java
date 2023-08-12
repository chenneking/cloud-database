package org.praktikum;

import org.praktikum.communication.MessageHandler;
import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.FrequencyTable;
import org.praktikum.resources.RingList;
import org.praktikum.resources.UsageMetrics;
import org.praktikum.storage.*;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class KVServer {
    private final String address;
    private final String bootstrapAddress;
    private ServerSocket serverSocket;
    private final int port;
    private boolean isRunning;
    private final KVStore store;
    private final Map<String, KVStore> replicaStores = new HashMap<>();
    public static final Logger log = Logger.getLogger("KVServer");
    private ECSConnection ecsConnection;
    private String startRange;
    private String endRange;
    private final RingList ringList;
    boolean writeLock = false;
    boolean isStopped = true;
    private final ConsistentHashing hashing;
    private final ArrayList<ClientConnection> clientConnections = new ArrayList<>();
    private final Random random;
    private String customEndRangeHash;
    private final FrequencyTable frequencyTable;
    private final UsageMetrics usageMetrics;

    /**
     * Constructs a KVServer instance with the given configurations.
     *
     * @param port                 The port on which the server will listen.
     * @param address              The IP address of the server.
     * @param bootstrapAddress     The address of the bootstrap server.
     * @param storageLocation      The directory for persistent storage files on the server.
     * @param logFilePath          The path for the log files on the server.
     * @param logLevel             The logging level for the server.
     * @param cacheSize            The cache size in terms of number of keys.
     * @param displacementStrategy The cache displacement strategy.
     * @param numberOfBuckets      The number of buckets for key range partitioning.
     * @param offloadThreshold     The offload threshold for key range transfer.
     * @param customEndRangeHash   Custom hash for end range.
     */
    public KVServer(int port, String address, String bootstrapAddress, String storageLocation, String logFilePath, Level logLevel, int cacheSize, String displacementStrategy, int numberOfBuckets, int offloadThreshold, String customEndRangeHash) {
        this.port = port;
        this.isRunning = false;
        this.address = address;
        //Hashing object to get the hash value of the port and ip
        try {
            this.hashing = new ConsistentHashing();
            String filename = hashing.getMD5Hash(address, Integer.toString(port));
            this.store = new KVStore(cacheSize, displacementStrategy, storageLocation, filename);

            log.setUseParentHandlers(false);
            FileHandler fileHandler = new FileHandler(logFilePath, true);
            fileHandler.setFormatter(new SimpleFormatter());
            log.addHandler(fileHandler);

            this.bootstrapAddress = bootstrapAddress;

            this.ringList = new RingList();
            this.random = new Random(port);
            this.frequencyTable = new FrequencyTable(numberOfBuckets, offloadThreshold);
            this.customEndRangeHash = customEndRangeHash;
            this.usageMetrics = new UsageMetrics();

        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KVStore getStore() {
        return store;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public Map<String, KVStore> getReplicaStores() {
        return replicaStores;
    }

    public String getStartRange() {
        return startRange;
    }

    public String getEndRange() {
        return endRange;
    }

    public RingList getRingList() {
        return ringList;
    }

    public boolean isWriteLock() {
        return writeLock;
    }

    public void setWriteLock(boolean writeLock) {
        this.writeLock = writeLock;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public void setStopped(boolean stopped) {
        isStopped = stopped;
    }

    /**
     * Manages key range and replication based on the current server's metadata.
     */
    public void fetchKeyRangesFromMetaData() {
        if (!ringList.isEmpty()) {
            RingList.Node server = ringList.findByIPandPort(address, Integer.toString(port));
            // boolean to check if we have a new predecessor -> this is important for the data transfer in the following code!
            boolean hasStartRangeChanged = false;
            if (server != null) {
                if (this.startRange != null) {
                    hasStartRangeChanged = !this.startRange.equals(server.getStartRange());
                }
                this.startRange = server.getStartRange();
                this.endRange = server.getEndRange();
                this.frequencyTable.updateBuckets(startRange, endRange);
            }
            boolean isReplicated = ringList.getSize() > 2;
            if (isReplicated && server != null) {
                if (hasStartRangeChanged) {
                    // add logic here to send a copy of the complete dataset in store to its own replicas (i.e an update)
                    RingList.Node serverWithReplicaOfSelf1 = server.getNext();
                    RingList.Node serverWithReplicaOfSelf2 = server.getNext().getNext();

                    try {
                        Socket socket1 = new Socket(serverWithReplicaOfSelf1.getIP(), Integer.parseInt(serverWithReplicaOfSelf1.getPort()));
                        MessageHandler socketMessageHandler1 = new MessageHandler(socket1);
                        Socket socket2 = new Socket(serverWithReplicaOfSelf2.getIP(), Integer.parseInt(serverWithReplicaOfSelf2.getPort()));
                        MessageHandler socketMessageHandler2 = new MessageHandler(socket2);

                        String allDataFromStore = store.getAllData();

                        socketMessageHandler1.send("replica_data_update " + getAddress() + " " + getPort() + " " + allDataFromStore);
                        System.out.println(new Date().getTime() + " Sent: " + "replica_data_update " + getAddress() + " " + getPort() + " " + allDataFromStore);
                        socketMessageHandler2.send("replica_data_update " + getAddress() + " " + getPort() + " " + allDataFromStore);
                        System.out.println(new Date().getTime() + " Sent: " + "replica_data_update " + getAddress() + " " + getPort() + " " + allDataFromStore);

                        socket1.close();
                        socket2.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                RingList.Node replica1 = server.getPrev();
                String replica1IPPortString = replica1.getIP() + ":" + replica1.getPort();
                boolean replica1AlreadyStored = replicaStores.containsKey(replica1IPPortString);

                RingList.Node replica2 = server.getPrev().getPrev();
                String replica2IPPortString = replica2.getIP() + ":" + replica2.getPort();
                boolean replica2AlreadyStored = replicaStores.containsKey(replica2IPPortString);

                // check if there's a change in replica servers
                createReplicaStoreAndRequestData(replica1, replica1IPPortString, replica1AlreadyStored);
                createReplicaStoreAndRequestData(replica2, replica2IPPortString, replica2AlreadyStored);

                List<String> toBeRemovedKeys = new LinkedList<>();

                for (String key : replicaStores.keySet()) {
                    if (!key.equals(replica1IPPortString) && !key.equals(replica2IPPortString)) {
                        KVStore toBeDeleted = replicaStores.get(key);
                        if (toBeDeleted.deleteAllData()) {
                            System.out.println("Data of replica " + key + " was deleted from this server");
                            toBeRemovedKeys.add(key);
                        } else {
                            System.out.println(new Date().getTime() + " ERROR: Data of replica " + key + " couldn't be deleted from this server");
                        }
                    }
                }

                for (String key : toBeRemovedKeys) {
                    replicaStores.remove(key);
                }
            } else if (!isReplicated && server != null && replicaStores.size() > 0) {
                System.out.println("We are not replicating our data anymore, because there are only 2 servers left");

                List<String> toBeRemovedKeys = deleteAllReplicaStores();
                for (String key : toBeRemovedKeys) {
                    replicaStores.remove(key);
                }
            }
        }
    }

    /**
     * Attempts to delete all data from each replica store and returns the keys of the replica stores
     * whose data was deleted successfully.
     *
     * @return A list of keys of the replica stores whose data was successfully deleted.
     */
    private List<String> deleteAllReplicaStores() {
        List<String> toBeRemovedKeys = new LinkedList<>();
        for (String key : replicaStores.keySet()) {
            KVStore toBeDeleted = replicaStores.get(key);
            if (toBeDeleted.deleteAllData()) {
                System.out.println("Data of replica " + key + " was deleted from this server");
                toBeRemovedKeys.add(key);
            } else {
                System.out.println(new Date().getTime() + " ERROR: Data of replica " + key + " couldn't be deleted from this server");
            }
        }
        return toBeRemovedKeys;
    }

    public ECSConnection getEcsConnection() {
        return ecsConnection;
    }

    /**
     * Creates a replica store for a given replica node, associates it with that node, and requests data for it.
     * This process is executed only if the replica isn't already stored.
     * After a new KVStore is created for the replica node, a socket connection is opened to the replica node,
     * a request is sent to fetch its replica data, and upon receiving the data, it is saved in the newly created KVStore.
     *
     * @param replicaNode          The node for which the replica store is to be created.
     * @param replicaIPPortString  The IP:Port string representation of the replica node.
     * @param replicaAlreadyStored A boolean indicating whether this replica is already stored in the current server.
     * @throws RuntimeException if there's an I/O error when establishing a socket connection or sending/receiving messages.
     */
    private void createReplicaStoreAndRequestData(RingList.Node replicaNode, String replicaIPPortString, boolean replicaAlreadyStored) {
        if (!replicaAlreadyStored) {
            String filename = hashing.getMD5Hash(replicaIPPortString) + random.nextInt(0, 2147483640);
            KVStore replica2Store = new KVStore(store.getCache().getMaxSize(), store.getDisplacementStrategy(), store.getStorageLocation(), filename);
            replicaStores.put(replicaIPPortString, replica2Store);
            System.out.println("KVStore for the replica of " + replicaIPPortString + " was created and has file name: " + replica2Store.getStorageLocation() + "/" + replica2Store.getFilename());
            // issue request to get data for this replica from the server via a socket
            try {
                Socket socket = new Socket(replicaNode.getIP(), Integer.parseInt(replicaNode.getPort()));
                MessageHandler socketMessageHandler = new MessageHandler(socket);
                // this is the Connected successfully! messages
                socketMessageHandler.receive();
                socketMessageHandler.send("request_replica_data");

                String response = new String(socketMessageHandler.receive(), StandardCharsets.UTF_8);

                System.out.println("Received (from " + replicaNode.getIP() + ":" + replicaNode.getPort() + "): " + response);

                replica2Store.saveData(response, false);
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void passNewMetaData(String data) {
        ringList.parseAndUpdateMetaData(data);
    }

    /**
     * Starts the server, executes a while loop which accepts any connecting Clients and starts a separate Thread which
     * is supposed to handle the connection to the client.
     */
    public void runServer() {
        try {
            InetAddress listeningAddress = InetAddress.getByName(address);
            serverSocket = new ServerSocket(port, 100000, listeningAddress);
            isRunning = true;

            connectECSServer();
            Signal.handle(new Signal("INT"), sig -> closingProtocol());
            Signal.handle(new Signal("TERM"), sig -> closingProtocol());
            while (isRunning) {
                Socket client = serverSocket.accept();
                try {
                    ConsistentHashing hashing = new ConsistentHashing();
                    ClientConnection connection = new ClientConnection(client, hashing, this);
                    clientConnections.add(connection);
                    new Thread(connection).start();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            log.warning("Error while accepting connection from " + address + ":" + port);
        }
    }

    /**
     * Connects the current server with the ECSServer.
     */
    public void connectECSServer() {
        try {
            String ecsAddress = bootstrapAddress.split(":")[0];
            String ecsPort = bootstrapAddress.split(":")[1];
            Socket ecs = new Socket(ecsAddress, Integer.parseInt(ecsPort));
            ConsistentHashing hashing = new ConsistentHashing();
            ECSConnection connection = new ECSConnection(ecs, store, hashing, this, address, port, customEndRangeHash);
            ecsConnection = connection;
            new Thread(connection).start();
        } catch (IOException e) {
            log.warning("Error while trying to connect to the ECSServer");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Initiates the server shutdown protocol.
     * This method ensures that the server's data is properly transferred to its successor
     * before the server node is taken down.
     */
    public void closingProtocol() {
        RingList.Node node = ringList.findByIPandPort(address, String.valueOf(port));
        String ipPrev = node.getNext().getIP();
        int portPrev = Integer.parseInt(node.getNext().getPort());

        ecsConnection.send("close");
        //makes sure to only send 128000 bytes
        String outPut = ecsConnection.getAllData();
        int length = outPut.length();

        try {
            if (ringList.getSize() != 1) {
                System.out.println("Attempting to connect to " + ipPrev + ":" + portPrev);
                Socket serverCommunication = new Socket(ipPrev, portPrev);
                MessageHandler messageHandler = new MessageHandler(serverCommunication);

                if (length >= 128000) {
                    int substringLength = (int) Math.ceil((double) length / 128000);
                    for (int i = 0; i < length; i += substringLength) {
                        int endIndex = Math.min(i + substringLength, length);
                        String substring = outPut.substring(i, endIndex);
                        messageHandler.send("save_data " + substring);
                    }
                } else {
                    messageHandler.send("save_data " + outPut);
                    System.out.println("sending: " + outPut);
                }
                store.deleteAllData();
            }
            deleteAllReplicaStores();
            ecsConnection.send("data_complete_send");
            stopServer();
        } catch (IOException e) {
            stopServer();
            throw new RuntimeException(e);
        }
    }

    /**
     * Stops the KVServer and closes all its connections.
     */
    public void stopServer() {
        isRunning = false;
        //puts all the remaining KVPairs from the cache into the storage
        store.flushCache();
        try {
            for (ClientConnection clientConnection : clientConnections) {
                clientConnection.close();
            }
            ecsConnection.close();
            serverSocket.close();
        } catch (IOException e) {
            log.warning("Error while closing server socket");
        }
        log.info("Stopped server");
    }

    public UsageMetrics getUsageMetrics() {
        return usageMetrics;
    }

    public FrequencyTable getFrequencyTable() {
        return frequencyTable;
    }

    public static void main(String[] args) {
        //KVServer kv = new KVServer(8001,"127.0.0.1","0.0.0.0:44331","123","123",Level.ALL,0,"FIFO", 5 ,30,null);
        //kv.runServer();
    }
}
