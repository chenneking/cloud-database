package org.praktikum;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;

public class PingCommunication implements Runnable {
    private final ECSMessageHandler messageHandler;
    private final String ip;
    private final String port;
    private final Socket clientSocket;

    /**
     * Constructor for the PingCommunication class.
     * Initializes the ECSMessageHandler with the provided client socket and sets the IP and port.
     *
     * @param clientSocket The socket for communication with the client.
     * @param ip           The IP address of the client.
     * @param port         The port number of the client.
     */
    public PingCommunication(Socket clientSocket, String ip, String port) {
        this.messageHandler = new ECSMessageHandler(clientSocket);
        this.clientSocket = clientSocket;
        this.ip = ip;
        this.port = port;
    }

    /**
     * Periodically sends "ping_request" messages to a server to check its availability.
     * <p>
     * Upon starting, a welcome message is read. Then, "ping_request" messages are
     * repeatedly sent to the server every 700 milliseconds. If no message is received
     * back from the server, it is concluded that the server is down.
     *
     * @throws RuntimeException if the thread is interrupted while sleeping.
     */
    @Override
    public void run() {
        try {
            //welcome message has to be read out
            byte[] input = messageHandler.receive();
            System.out.println("started pingRequest for: " + port + " Port: " + ip);

            while (true) {
                messageHandler.send("ping_request");
                Thread.sleep(700);
                int receivedSize = messageHandler.size();
                if (receivedSize <= 0) {
                    break;
                }
                //read the data out of the input stream
                input = messageHandler.receive();
            }
            closeConnection();
            System.out.println("server with IP: " + ip + " and Port: " + port + " is down");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Closes the connection with the client.
     * <p>
     * This involves setting the isOpen flag to false, closing the message handler,
     * and then closing the client socket. If any issues arise while closing the client socket,
     * a RuntimeException is thrown.
     */
    public void closeConnection() {
        messageHandler.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
