package org.praktikum.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RingList {
    Node head;
    int size = 0;

    private final MessageDigest digest;

    /**
     * Constructor for the RingList class.
     * Initializes the digest with an MD5 hash algorithm.
     *
     * @throws NoSuchAlgorithmException if the "MD5" algorithm isn't available.
     */
    public RingList() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("MD5");
    }

    /**
     * Adds a new node to the ring list based on the provided IP, port, and hash string.
     * If no hash string is provided, an MD5 hash of the IP and port is used.
     *
     * @param IP         The IP address of the node to be added.
     * @param port       The port number of the node to be added.
     * @param hashString A custom end range hash string provided during server startup. If null, a new hash will be generated.
     * @return The newly added node.
     */
    public synchronized Node add(String IP, String port, String hashString) {
        // If no custom hash string has been provided, calculate it based on the old logic.
        if (hashString == null) {
            hashString = getMD5Hash(IP, port);
        }

        Node previousNode = find(hashString);

        Node newNode;
        if (previousNode == null) {
            newNode = new Node(IP, port, hashString);
        } else {
            newNode = new Node(previousNode, IP, port, hashString);
        }
        // Adjusts the head pointer if the list is currently empty.
        if (head == null) {
            head = newNode;
        }

        size++;

        return newNode;
    }


    /**
     * Removes a node from the ring list based on the provided hash string.
     * Adjusts the neighboring nodes and the head of the list accordingly.
     * The logic for removal varies based on the current size of the list.
     *
     * @param hashString The hash of the node to be removed.
     * @return The node that was removed, or null if the list was empty.
     */
    public synchronized Node remove(String hashString) {
        if (size == 0) {
            return null;
        } else if (size == 1) {
            Node node = head;
            head = null;
            size--;
            return node;
        }

        Node toBeRemovedNode = find(hashString);

        if (size == 2) {
            Node prev = toBeRemovedNode.getPrev();
            prev.setNext(prev);
            prev.setPrev(prev);
            prev.setEndRange(toBeRemovedNode.getEndRange());
            if (toBeRemovedNode == head) {
                head = prev;
            }
        } else {
            toBeRemovedNode.getPrev().setNext(toBeRemovedNode.getNext());
            toBeRemovedNode.getPrev().setEndRange(toBeRemovedNode.getEndRange());
            toBeRemovedNode.getNext().setPrev(toBeRemovedNode.getPrev());

            if (toBeRemovedNode == head) {
                head = toBeRemovedNode.getPrev();
            }
        }
        size--;
        return toBeRemovedNode;
    }

    /**
     * Generates an MD5 hash based on a server's IP and port.
     * The resulting hash is represented as a 32-character hexadecimal string.
     *
     * @param IP   The IP address for which the hash needs to be generated.
     * @param port The port number for which the hash needs to be generated.
     * @return The MD5 hash of the IP and port.
     */
    public String getMD5Hash(String IP, String port) {
        byte[] hash = this.digest.digest((IP + ":" + port).getBytes());
        BigInteger bigInteger = new BigInteger(1, hash);
        return String.format("%032X", bigInteger);
    }

    /**
     * Removes a node from the ring list based on the provided IP and port.
     * Adjusts the neighboring nodes and the head of the list accordingly.
     * The logic for removal varies based on the current size of the list.
     *
     * @param IP   The IP address of the node to be removed.
     * @param port The port number of the node to be removed.
     * @return The node that was removed, or null if the list was empty.
     */
    public synchronized Node remove(String IP, String port) {
        if (size == 0) {
            return null;
        } else if (size == 1) {
            Node node = head;
            head = null;
            size--;
            return node;
        }

        Node toBeRemovedNode = find(IP, port);

        if (size == 2) {
            Node prev = toBeRemovedNode.getPrev();
            prev.setNext(prev);
            prev.setPrev(prev);
            prev.setEndRange(toBeRemovedNode.getEndRange());
            if (toBeRemovedNode == head) {
                head = prev;
            }
        } else {
            toBeRemovedNode.getPrev().setNext(toBeRemovedNode.getNext());
            toBeRemovedNode.getPrev().setEndRange(toBeRemovedNode.getEndRange());
            toBeRemovedNode.getNext().setPrev(toBeRemovedNode.getPrev());

            if (toBeRemovedNode == head) {
                head = toBeRemovedNode.getPrev();
            }
        }
        size--;
        return toBeRemovedNode;
    }


    /**
     * Searches for a node in the ring list based on a given hash key.
     * The method traverses the list and returns the node whose start and end hash ranges encompass the provided key.
     *
     * @param key The hash key used to search for a node.
     * @return The node that matches the criteria or null if no such node exists.
     */
    public Node find(String key) {
        if (size == 0) {
            return null;
        }
        // Special Case when there is only one server, as it covers ALL values. I.e. no handling of inclusive/exclusive bounds.
        else if (size == 1) {
            return head;
        }

        Node node = head;
        do {
            if (node.getStartRange().compareTo(key) < 0 && node.getEndRange().compareTo(key) >= 0) {
                return node;
            } else if (node.getStartRange().compareTo(node.getEndRange()) >= 0) {
                if (node.getStartRange().compareTo(key) < 0 && node.getEndRange().compareTo(key) <= 0) {
                    return node;
                } else if (node.getStartRange().compareTo(key) > 0 && node.getEndRange().compareTo(key) >= 0) {
                    return node;
                }
            }
            node = node.getNext();
        } while (node != head);
        return null;
    }

    /**
     * Searches for a node in the ring list based on the provided IP address and port.
     * The method traverses the list and returns the node that matches the given IP address and port.
     *
     * @param address The IP address used to search for a node.
     * @param port    The port number used to search for a node.
     * @return The node that matches the provided IP address and port or null if no such node exists.
     */
    public Node find(String address, String port) {
        if (size == 0) {
            return null;
        }
        // Special Case when there is only one server, as it covers ALL values. I.e. no handling of inclusive/exclusive bounds.
        else if (size == 1) {
            return head;
        }

        Node node = head;
        do {
            if (node.getPort().equals(port) && node.getIP().equals(address)) {
                return node;
            }
            node = node.getNext();
        } while (node != head);
        return null;
    }

    /**
     * Updates the hash ranges of a node identified by the provided IP address and port.
     * This method updates the start and end hash ranges of the specified node and adjusts the neighboring nodes accordingly.
     *
     * @param ip         The IP address of the node to be updated.
     * @param port       The port number of the node to be updated.
     * @param startRange The new start hash range for the node.
     * @param endRange   The new end hash range for the node.
     */
    public synchronized void updateKeyRanges(String ip, String port, String startRange, String endRange) {
        Node node = find(ip, port);
        // Update startRange
        node.setStartRange(startRange);
        node.getPrev().setEndRange(startRange);

        //Update endRange
        node.setEndRange(endRange);
        node.getNext().setStartRange(endRange);
    }

    /**
     * Provides a string representation of the ring list.
     * The method iterates over the list and builds a string that displays the hash ranges and addresses of each node.
     *
     * @return A concatenated string representation of the ring list. Returns null if the list is empty.
     */

    @Override
    public String toString() {
        if (size == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder();

        Node node = head;

        do {
            builder.append(node.getStartRange());
            builder.append(",");
            builder.append(node.getEndRange());
            builder.append(",");
            builder.append(node.getIP());
            builder.append(":");
            builder.append(node.getPort());
            builder.append(";");
            node = node.next;
        } while (node != head);


        return builder.toString();
    }

    public static class Node {
        private Node prev;
        private Node next;
        private String IP;
        private String port;
        private String startRange;
        private String endRange;

        /**
         * Constructs a standalone node with the given IP, port, and hash range.
         * The node's next and previous pointers initially point to itself.
         *
         * @param IP       IP address of the server.
         * @param port     Port of the server.
         * @param endRange The hash range of the server.
         */
        public Node(String IP, String port, String endRange) {
            this.prev = this;
            this.next = this;
            this.IP = IP;
            this.port = port;
            this.startRange = endRange;
            this.endRange = endRange;
        }

        /**
         * Constructs a node and inserts it before the given 'prev' node in the list.
         *
         * @param prev     The reference node before which the new node will be inserted.
         * @param IP       IP address of the server.
         * @param port     Port of the server.
         * @param endRange The hash range of the server.
         */
        public Node(Node prev, String IP, String port, String endRange) {
            this.IP = IP;
            this.port = port;

            this.prev = prev.getPrev();
            this.next = prev;
            prev.getPrev().setNext(this);
            prev.setPrev(this);


            this.startRange = prev.getStartRange();
            this.endRange = endRange;
            this.getNext().setStartRange(endRange);
        }

        public Node getPrev() {
            return prev;
        }

        public void setPrev(Node prev) {
            this.prev = prev;
        }

        public Node getNext() {
            return next;
        }

        public void setNext(Node next) {
            this.next = next;
        }

        public String getIP() {
            return IP;
        }

        public void setIP(String IP) {
            this.IP = IP;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getStartRange() {
            return startRange;
        }

        public void setStartRange(String startRange) {
            this.startRange = startRange;
        }

        public String getEndRange() {
            return endRange;
        }

        public void setEndRange(String endRange) {
            this.endRange = endRange;
        }

        @Override
        public String toString() {
            return getIP() + "|" + getPort() + "|" + getStartRange() + "|" + getEndRange();
        }
    }
}
