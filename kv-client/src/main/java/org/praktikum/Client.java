package org.praktikum;

import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.RingList;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Client implements SignalHandler {
    private Socket client;
    private boolean isConnected = false;
    private InputStream in;
    private OutputStream out;
    private Level logLevel = log.getLevel();
    private static final String PROMPT = "EchoClient> ";
    private final RingList ringList;
    private static final Logger log = Logger.getLogger("EchoClient");

    private final BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

    private static ConsistentHashing hashing;

    private static boolean EXECUTE = true;

    private int retryCount = 0;
    private static final Random random = new Random(24058300);


    /**
     * Initializes the client and its necessary components including consistent hashing and ring list.
     * Additionally, adds a shutdown hook to ensure the client disconnects gracefully upon termination.
     * It also registers a signal handler for the TERM signal.
     */
    public Client() {
        try {
            hashing = new ConsistentHashing();
            ringList = new RingList();
            Runtime runtime = Runtime.getRuntime();
            runtime.addShutdownHook(new Thread(this::closeConnection));
            Signal termSignal = new Signal("TERM");
            Signal.handle(termSignal, this);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Main execution loop of the client. Continuously prompts the user for input commands
     * and executes them until the client is terminated.
     */
    private void run() {
        while (EXECUTE) {
            System.out.print(PROMPT);
            try {
                String line = input.readLine();
                String[] tokens = line.trim().split("\\s+");
                executeCommand(tokens);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Validates and delegates user input commands for execution.
     *
     * @param tokens Array of string tokens that represent a user command and its arguments.
     */
    private void executeCommand(String[] tokens) {
        if (tokens.length == 0) {
            error();
        }
        switch (tokens[0]) {
            case "connect" -> {
                if (tokens.length != 3 || tokens[1] == null || tokens[2] == null) {
                    error();
                    break;
                }
                if (isConnected) {
                    print("You are already connected to " + client.getInetAddress().getHostName() + ":" + client.getPort());
                    break;
                }
                try {
                    String host = tokens[1];
                    int port = Integer.parseInt(tokens[2]);
                    connect(host, port);
                } catch (NumberFormatException e) {
                    print("You provided an invalid port number! Please try again.");
                }
            }
            case "disconnect" -> {
                if (tokens.length > 1) {
                    error();
                    break;
                }
                if (!isConnected) {
                    print("There is no active connection to be closed.");
                    break;
                }
                closeConnection();
                print("Disconnected from server.");
            }
            case "send" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                if (tokens.length < 2) {
                    error();
                    break;
                }
                String[] dataToSend = Arrays.copyOfRange(tokens, 1, tokens.length);
                String data = String.join(" ", dataToSend);
                sendString(data);
                handleServerResponse(tokens);
            }
            case "put" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                if (tokens.length < 3) {
                    error();
                    break;
                }

                updateClientConnectionIfRequired(tokens[1]);
                put(tokens[1], tokens[2]);
                handleServerResponse(tokens);
            }
            case "get" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                if (tokens.length < 2) {
                    error();
                    break;
                }

                updateClientConnectionIfRequiredGet(tokens[1]);
                get(tokens[1]);
                handleServerResponse(tokens);
            }
            case "delete" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                if (tokens.length < 2) {
                    error();
                    break;
                }
                updateClientConnectionIfRequired(tokens[1]);
                delete(tokens[1]);
                handleServerResponse(tokens);

            }
            case "keyrange" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }

                requestKeyRange();
                handleServerResponse(tokens);
            }
            case "keyrange_read" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                requestKeyRangeRead();
                handleServerResponse(tokens);
            }

            case "logLevel" -> {
                if (tokens.length > 2) {
                    error();
                    break;
                }
                try {
                    Level newLevel = Level.parse(tokens[1]);
                    log.setLevel(newLevel);
                    if (logLevel == null) {
                        print("loglevel set from none to " + newLevel.getName());
                    } else {
                        print("loglevel set from" + logLevel.getName() + " to " + newLevel.getName());
                    }
                    logLevel = newLevel;
                } catch (Exception e) {
                    error();
                }
            }
            case "help" -> printHelpText();
            case "quit" -> {
                print("The client application will shutdown now.");
                boolean connectionClosed = closeConnection();
                if (!connectionClosed) {
                    break;
                }
                EXECUTE = false;
            }
            case "get_frequency_table" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                sendString("get_frequency_table");
                handleServerResponse(tokens);

            }
            case "get_usage_metrics" -> {
                if (!isConnected) {
                    print("You aren't connected to a server yet! Please connect to a server using the connect command first.");
                    break;
                }
                sendString("get_usage_metrics");
                handleServerResponse(tokens);

            }
            default -> {
                print("Unknown command! Please refer to help to find information on the proper command usage:");
                printHelpText();
            }
        }
    }

    /**
     * Updates the client connection if not connected to the appropriate server for a given key.
     *
     * @param key The key to determine the appropriate server.
     */
    private void updateClientConnectionIfRequired(String key) {
        // Don't update the connection if the client has no Metadata in store.
        if (ringList.isEmpty()) {
            return;
        }

        String keyHash = hashing.getMD5Hash(key);
        RingList.Node node = ringList.findByHashKey(keyHash);

        if (node != null && (!client.getInetAddress().getHostAddress().equals(node.getIP()) || !Integer.toString(client.getPort()).equals(node.getPort()))) {
            int port = Integer.parseInt(node.getPort());
            closeConnection();
            connect(node.getIP(), port);
        }
    }

    /**
     * Updates the client connection based on the provided key and handles the replica logic.
     *
     * @param key The key to determine the appropriate server and replica logic.
     */
    private void updateClientConnectionIfRequiredGet(String key) {
        //handle case where metadata isn't available to client i.e it's empty
        if (ringList.isEmpty()) {
            return;
        }

        String keyHash = hashing.getMD5Hash(key);

        // RingList.Node random_replica = ringList.getRandomNodeFromKey(keyHash);
        RingList.Node node = ringList.findByHashKey(keyHash);
        if (ringList.getFromReplica(client.getInetAddress().getHostAddress(), Integer.toString(client.getPort()), keyHash)) {
            int port = Integer.parseInt(node.getPort());
            closeConnection();
            connect(node.getIP(), port);
        }
    }

    /**
     * Retrieves and prints the server's response.
     */
    private void printServerAnswer() {
        byte[] received = receive();
        if (received != null) {
            String receivedString = new String(received);
            System.out.print(PROMPT + receivedString);
        }
    }

    /**
     * Sends a String of data to the connected server
     *
     * @param data String of data
     */
    private void sendString(String data) {
        data = data + "\r\n";
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        send(bytes);
    }

    /**
     * Establishes a connection to a server using the provided host and port.
     *
     * @param host The hostname or IP address of the server.
     * @param port The port number of the server.
     */
    private void connect(String host, int port) {
        try {
            client = new Socket(host, port);
            in = client.getInputStream();
            out = client.getOutputStream();

            isConnected = true;

            log.info("Connected to server at " + client.getInetAddress().getHostName() + ":" + client.getPort());
            byte[] received = receive();
            if (received != null) {
                System.out.print(PROMPT + new String(received));
            }

        } catch (IOException e) {
            print("An error occurred while the creating the connection! Please try again.");
            print(e.getMessage());
            log.warning("Error while trying to connect to " + client.getInetAddress().getHostName() + ":" + client.getPort());
        }
    }

    //client put,get and delete wrapper for benchmarking
    public void putPublic(String key, String value) {
        executeCommand(new String[]{"put", key, value});
    }

    //Todo die raus oder
    public void getPublic(String key) {
        executeCommand(new String[]{"get", key});
    }

    public void deletePublic(String key, String value) {
        executeCommand(new String[]{"delete", key, value});
    }

    public void connectPublic(String address, int port) {
        executeCommand(new String[]{"connect", address, Integer.toString(port)});

    }

    /**
     * Handles the server's response after a command execution. This includes handling
     * scenarios like server redirection, server not responsible, and more.
     *
     * @param tokens Tokens of the executed command.
     */
    private void handleServerResponse(String[] tokens) {
        byte[] received = receive();
        if (received != null) {
            String receivedString = new String(received);
            if (receivedString.startsWith("server_stopped")) {
                try {
                    Thread.sleep(getBackoff() * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executeCommand(tokens);
            } else if (receivedString.startsWith("server_not_responsible")) {
                //System.out.print(PROMPT + receivedString);
                requestKeyRange();
                handleKeyRangeResponse();
                executeCommand(tokens);
            } else if (receivedString.startsWith("server_write_lock")) {
                print("The server is currently write locked. Please try again later.");
            } else if (receivedString.startsWith("keyrange_success")) {
                String[] keyRangeData = receivedString.split("keyrange_success ");
                if (keyRangeData[1] != null) {
                    ringList.parseAndUpdateMetaData(keyRangeData[1]);
                } else {
                    log.warning("There was an error while parsing the received keyranges.");
                }
                System.out.print(PROMPT + receivedString);
                this.retryCount = 0;
            } else {
                System.out.print(PROMPT + receivedString);
                this.retryCount = 0;
            }
        }
    }

    /**
     * Computes the exponential backoff time with jitter to handle retries.
     *
     * @return The backoff time in milliseconds.
     */
    private long getBackoff() {
        int backoff = (int) Math.pow(2, retryCount++);
        int jitter = random.nextInt((int) Math.ceil(backoff >> 1));
        return backoff + jitter;
    }


    /**
     * Handles the server's response to a key range request by updating the client's knowledge of the ring list.
     */
    private void handleKeyRangeResponse() {
        byte[] received = receive();
        if (received != null) {
            String receivedString = new String(received);
            System.out.print(PROMPT + receivedString);
            String[] keyRangeData = receivedString.split("keyrange_success ");
            if (keyRangeData[1] != null) {
                ringList.parseAndUpdateMetaData(keyRangeData[1]);
            } else {
                log.warning("There was an error while parsing the received keyranges.");
            }
        }
    }

    /**
     * Retrieves data sent by the server.
     *
     * @return A byte array containing the received data, or null if there's no data or a connection issue.
     */
    private byte[] receive() {
        if (!isConnected) {
            return null;
        }
        try {
            byte prev = (byte) in.read();
            int i = 1;
            byte[] bytes = new byte[128000];
            bytes[0] = prev;
            while (true) {
                byte curr = (byte) in.read();
                bytes[i++] = curr;
                //condition to break out of loop when we've reached the end of a message
                if (prev == (byte) 13 && curr == (byte) 10) {
                    break;
                }
                prev = curr;
            }
            byte[] returnArray = Arrays.copyOfRange(bytes, 0, i);
            log.info("Received from server: " + new String(returnArray));
            return returnArray;
        } catch (IOException e) {
            print("An error occurred while receiving data from the server.");
        }
        return null;
    }

    /**
     * Sends data to the connected server
     *
     * @param data A byte array of data
     */
    private void send(byte[] data) {
        try {
            out.write(data);
            out.flush();
            log.info("Sent to server: " + new String(data));
        } catch (IOException e) {
            print("An error occurred while sending data from the server.");
        }
    }

    /**
     * Closes down the established socket connection
     *
     * @return boolean value if operation was successful
     */
    private boolean closeConnection() {
        if (isConnected) {
            try {
                sendString("closing_client");
                in.close();
                out.close();
                client.close();
                isConnected = false;
                log.info("Disconnected from server " + client.getInetAddress().getHostName() + ":" + client.getPort());
            } catch (Exception e) {
                print("An error occurred while the closing the connection! Please try again.");
                log.warning("Error while trying to disconnect from " + client.getInetAddress().getHostName() + ":" + client.getPort());
                isConnected = false;
                return false;
            }
        }
        return true;
    }

    /**
     * Sends a put request for a <key><value> pair
     *
     * @param key   String <key>
     * @param value String <value>
     */
    private void put(String key, String value) {
        sendString("put " + key + " " + value);
    }

    /**
     * Sends a get request for a <key>
     *
     * @param key String <key>
     */
    private void get(String key) {
        sendString("get " + key);
    }

    /**
     * Sends a delete request for a <key>
     *
     * @param key String <key>
     */
    private void delete(String key) {
        sendString("delete " + key);
    }

    /**
     * Sends the keyrange request
     */
    public void requestKeyRange() {
        sendString("keyrange");
    }

    /**
     * Sends the keyrange_read request
     */
    public void requestKeyRangeRead() {
        sendString("keyrange_read");
    }

    /**
     * Prints out a formatted help text to the user, explaining the usage of each available command.
     */
    public static void printHelpText() {
        System.out.println(PROMPT + "List of Commands:");
        printHelpCommand(
                "connect",
                "Establishes a connection with a remote echo server",
                "connect <hostname> <port>",
                "connect cdb.dis.cit.tum.de 5551",
                "hostname - hostname / ip address of the echo server",
                "port - Port of the echo service on the server"
        );
        printHelpCommand(
                "disconnect",
                "Breaks down connection to previously connected remote echo server",
                null,
                null
        );
        printHelpCommand(
                "send", "Send a message to the remote echo server",
                "send <message>",
                "send Hello World!",
                "message - The message sent to the server"
        );
        printHelpCommand(
                "logLevel", "Adjusts the current log level of the logger",
                "logLevel <level>",
                "logLevel INFO",
                "level - Loglevel (ALL/CONFIG/FINE/FINEST/INFO/OFF/SEVERE/WARNING)"
        );
        printHelpCommand(
                "help",
                "Prints help text",
                null,
                null
        );
        printHelpCommand(
                "quit",
                "Breaks down connection to the previously connected remote echo server and exits the program",
                null,
                null
        );
    }

    private static void printHelpCommand(String command, String description, String usage, String example, String... parameters) {
        System.out.format("\n%-12s%s", command, description);
        if (usage != null) {
            System.out.format("\n\t\t\t%s", "Usage:      " + usage);
            System.out.format("\n\t\t\t%s", "Parameters: " + String.join("\n\t\t\t            ", parameters));
            System.out.format("\n\t\t\t%s", "Example:    " + example);
        }
        System.out.println();
    }


    /**
     * Utility method to print an error message when an invalid command is entered.
     */
    public static void error() {
        print("Invalid Input! Please refer to help to find information on the proper command usage.");
    }

    @Override
    public void handle(Signal sig) {
        closeConnection();
    }

    /**
     * Prints the provided output with the client's prompt.
     *
     * @param output The string to be printed.
     */
    public static void print(String output) {
        System.out.println(PROMPT + output);
    }

    public static void main(String[] args) {
        try {
            log.setUseParentHandlers(false);

            String logFilePath = "logs/client.log";

            // Create the logs directory if it doesn't exist yet.
            Path path = Paths.get(logFilePath);
            if (!Files.exists(path.getParent())) {
                Files.createDirectories(path.getParent());
            }

            FileHandler fileHandler = new FileHandler(logFilePath, true);
            fileHandler.setFormatter(new SimpleFormatter());
            log.addHandler(fileHandler);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Client client = new Client();
        client.run();
    }
}
