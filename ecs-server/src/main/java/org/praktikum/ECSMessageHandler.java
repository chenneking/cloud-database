package org.praktikum;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ECSMessageHandler {
    private InputStream in;
    private OutputStream out;


    public ECSMessageHandler(Socket ECSConnection) {
        try {
            in = ECSConnection.getInputStream();
            out = ECSConnection.getOutputStream();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    /**
     * Retrieves a set of bytes sent by the connected client
     *
     * @return A byte array of ASCII characters
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
                if (curr == - 1 || curr == - 3) {
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
     * Sends data to the connected client
     *
     * @param data String with the needed data
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
     * Returns the number of bytes that can be read (or skipped over) from the input stream without blocking.
     *
     * @return An estimate of the number of bytes that can be read (or skipped over) from the input stream without blocking.
     * @throws RuntimeException if an I/O error occurs when calling the available method of the input stream.
     */

    public int size() {
        try {
            return in.available();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * tries to close in the in and output stream and catches potential exceptions
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
