package org.praktikum.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class RingList {
    Node head;
    int size = 0;

    private final MessageDigest digest;

    /**
     * Default constructor that initializes the MessageDigest with the MD5 hashing algorithm.
     *
     * @throws NoSuchAlgorithmException if the MD5 hashing algorithm is not available.
     */
    public RingList() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("MD5");
    }


    public int getSize() {
        return size;
    }

    /**
     * Adds a new node to the ringlist based on the provided IP, port, and hash string.
     * If the hash string is not provided (i.e., null), the method will compute an MD5 hash
     * from the given IP and port. If a node with the given hash string already exists,
     * the new node will be positioned next to the existing node.
     *
     * @param IP         The IP address of the server to be added to the ring.
     * @param port       The port number of the server to be added to the ring.
     * @param hashString The hash string representing the position of the node in the ring.
     *                   If null, the method will compute a hash using the provided IP and port.
     * @return The newly created and added node.
     */
    public synchronized Node add(String IP, String port, String hashString) {
        if (hashString == null) {
            hashString = getMD5Hash(IP, port);
        }

        Node previousNode = findByHashKey(hashString);

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
     * Removes the node with the given hash from the list.
     * <p>
     * This method removes the node with the specified hash from the list, adjusting the neighboring nodes and the head of the list accordingly.
     * If the list has 0 nodes, it simply returns null. If the list has 1 node, it removes that node and sets head to null. If the list has 2 nodes,
     * it removes the appropriate node and updates the remaining node's next and previous references to point to itself. For a list with more than 2 nodes,
     * it removes the appropriate node and updates the next and previous references of the neighboring nodes. If the head of the list is being removed,
     * the head reference is updated to point to the previous node.
     *
     * @param hashString The hash of the node to be removed.
     * @return The removed node. Returns null if the list was empty.
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

        Node toBeRemovedNode = findByHashKey(hashString);

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
     * Generates an MD5 hash for a server's IP and port.
     *
     * @param IP   The IP address of the server.
     * @param port The port number of the server.
     * @return A 32-character hexadecimal string representing the MD5 hash of the server's IP and port.
     */
    public String getMD5Hash(String IP, String port) {
        byte[] hash = this.digest.digest((IP + ":" + port).getBytes());
        BigInteger bigInteger = new BigInteger(1, hash);
        return String.format("%032X", bigInteger);
    }

    /**
     * Removes a node from the ring list based on the given IP and port.
     *
     * @param IP   The IP address of the node to be removed.
     * @param port The port of the node to be removed.
     * @return The removed node or null if the node is not found.
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

        Node toBeRemovedNode = findByIPandPort(IP, port);

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
     * Searches for a node in the ring list based on the provided hash key. It iterates
     * through the list until it finds a node that matches the key based on the start and
     * end ranges.
     *
     * @param key The hash key to search for.
     * @return The node that matches the key or null if no such node exists.
     */
    public Node findByHashKey(String key) {
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
     *
     * @param address The IP address to search for.
     * @param port    The port number to search for.
     * @return The node that matches the provided IP address and port or null if no such node exists.
     */
    public Node findByIPandPort(String address, String port) {
        if (size == 0) {
            return null;
        }
        Node node = head;
        do {
            if (node.getIP().equals(address) && node.getPort().equals(port)) {
                return node;
            }
            node = node.getNext();
        } while (node != head);
        return null;
    }

    /**
     * Determines if the node with the specified address and port is one of the potential nodes
     * that can be used to read the value associated with the given key.
     *
     * @param address IP address of the node.
     * @param port    Port number of the node.
     * @param key     The hash key to be looked up.
     * @return true if the node with the given address and port is one of the replicas
     * that might have the value for the given key.
     */
    public boolean getFromReplica(String address, String port, String key) {
        Node should_node = findByHashKey(key);
        Node currentNode = findByIPandPort(address, port);
        Node replica1 = currentNode.next;
        Node replica2 = replica1.next;
        return should_node == currentNode || should_node == replica1 || should_node == replica2;
    }


    /**
     * Retrieves a random replica node from the ring list for a given key.
     * If the size of the ring list is greater than 2, it randomly selects among
     * the primary node and its two replicas; otherwise, it simply returns the primary node.
     *
     * @param key The hash key based on which a node is to be selected.
     * @return The node corresponding to the given hash key or one of its replicas.
     */
    public Node getRandomNodeFromKey(String key) {
        if (this.size > 2) {
            Random random = new Random();
            int i = random.nextInt(3);
            if (i == 0) {
                return findByHashKey(key);
            }
            if (i == 1) {
                return findByHashKey(key).next;
            }
            return findByHashKey(key).next.next;
        } else return findByHashKey(key);
    }

    /**
     * Provides a string representation of the ring list, detailing each node's range and server details.
     *
     * @return A concatenated string of all nodes' details in the format "startRange,endRange,IP:Port;".
     * Returns null if the ring list is empty.
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

    /**
     * Checks if the ring list is currently empty.
     *
     * @return true if the ring list has no nodes; false otherwise.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Parses the provided metadata string to update the ring list's state.
     * This involves resetting the current state and reconstructing the ring list
     * based on the given metadata.
     *
     * @param data Metadata in the format "startRange,endRange,IP:Port;" for each server.
     */
    public void parseAndUpdateMetaData(String data) {
        if (data.equals("null")) {
            return;
        }
        head = null;
        size = 0;
        String[] splitted = data.split(";");
        for (int i = 0; i < splitted.length - 1; i++) {
            String valueString = splitted[i];
            String[] values = valueString.split(",");
            String[] server = values[2].split(":");
            add(server[0], server[1], values[1]);
        }
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