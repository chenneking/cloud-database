package org.praktikum;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ECSMessageHandler {
    private InputStream in;
    private OutputStream out;

    /**
     * Constructor for ECSMessageHandler.
     * <p>
     * Initializes the input and output streams associated with the provided socket connection.
     *
     * @param ECSConnection The socket connection with the client.
     */
    public ECSMessageHandler(Socket ECSConnection) {
        try {
            in = ECSConnection.getInputStream();
            out = ECSConnection.getOutputStream();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    /**
     * Receives a message from the connected client.
     * <p>
     * Reads the incoming message byte by byte and returns it as a byte array.
     * The method looks for the CRLF sequence (i.e., \r\n) to determine the end of the message.
     * If the end of the stream is reached or the socket is closed, the method closes the connection and returns null.
     *
     * @return A byte array representing the received message or null if an error occurs.
     */
    public byte[] receive() {
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
                if (curr == -1 || curr == -3) {
                    close();
                    return null;
                }
                prev = curr;
            }
            return Arrays.copyOfRange(bytes, 0, i);
        } catch (IOException e) {
            ECSServer.log.warning("IOException occurred while receiving data through socket.");
        }
        return null;
    }

    /**
     * Sends a message to the connected client.
     * <p>
     * The message is prefixed with "ECS " and suffixed with CRLF (i.e., "\r\n") before sending.
     *
     * @param data The message to be sent.
     */
    public void send(String data) {
        try {
            data = "ECS " + data + "\r\n";
            out.write(data.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            ECSServer.log.warning("IOException occurred while sending data through socket.");
        }
    }

    /**
     * Determines the number of bytes available to be read from the input stream.
     *
     * @return The number of bytes available to be read.
     * @throws RuntimeException if there's an I/O error while determining available bytes.
     */
    public int size() {
        try {
            return in.available();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes the input and output streams.
     * <p>
     * This method tries to close both streams and logs any exceptions that may occur during the process.
     */
    public void close() {
        try {
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
