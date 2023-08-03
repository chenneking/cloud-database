package org.praktikum.communication;

import org.praktikum.KVServer;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.logging.Logger;

public class MessageHandler {
    private InputStream in;
    private OutputStream out;

    public MessageHandler(Socket clientConnection) {
        try {
            in = clientConnection.getInputStream();
            out = clientConnection.getOutputStream();
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
            while (i < 128000) {
                byte curr = (byte) in.read();
                bytes[i++] = curr;
                if (curr == - 1 || curr == - 3) {
                    close();
                    return null;
                }
                //condition to break out of loop when we've reached the end of a message
                if (prev == (byte) 13 && curr == (byte) 10) {
                    break;
                }
                prev = curr;
            }
            return Arrays.copyOfRange(bytes, 0, i);
        } catch (IOException e) {
            KVServer.log.warning("IOException occurred while receiving data through socket.");
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
            data = data + "\r\n";
            out.write(data.getBytes(StandardCharsets.UTF_8));
            out.flush();
            if (! data.startsWith("server_is_running")) {
                sent(data);
            }
        } catch (IOException e) {
            //do sth
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

    /**
     * prints the given string
     *
     * @param string
     */
    private void sent(String string) {
        System.out.println("Sent: " + string);
    }

}
