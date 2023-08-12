package org.praktikum.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class RingList {
    Node head;
    int size = 0;
    private final MessageDigest digest;

    /**
     * Constructs a new RingList initialized with an MD5 hashing mechanism.
     *
     * @throws NoSuchAlgorithmException if MD5 algorithm is not available.
     */
    public RingList() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("MD5");
    }

    public int getSize() {
        return size;
    }

    /**
     * Adds a node to the ring list based on the given IP, port, and hashString.
     *
     * @param IP         The IP address for the node.
     * @param port       The port number for the node.
     * @param hashString The hash string for the node. If null, it will be generated.
     * @return The newly added node.
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
        //creates new Node and automatically adjusts the respective pointers.
        if (head == null) {
            head = newNode;
        }

        size++;

        return newNode;
    }

    /**
     * Removes a node with the given hash from the list.
     *
     * @param hashString The hash string of the node to be removed.
     * @return The removed node or null if not found.
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
     * Generates an MD5 hash for a given IP and port.
     *
     * @param IP   The IP address.
     * @param port The port number.
     * @return The MD5 hash string.
     */

    public String getMD5Hash(String IP, String port) {
        byte[] hash = this.digest.digest((IP + ":" + port).getBytes());
        BigInteger bigInteger = new BigInteger(1, hash);
        return String.format("%032X", bigInteger);
    }

    /**
     * Removes a node with the specified IP and port from the ring list.
     *
     * @param IP   The IP address of the node.
     * @param port The port number of the node.
     * @return The removed node or null if not found.
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
     * Finds a node in the ring list based on a hash key.
     *
     * @param key The hash key to search for.
     * @return The found node or null if non-existent.
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
     * Finds a node in the ring list by its IP address and port.
     *
     * @param address The IP address of the node.
     * @param port    The port number of the node.
     * @return The found node or null if non-existent.
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
     * Generates a string representation of the ring list,
     * including replication range and server IP and port.
     *
     * @return A string representation of the ring list's key range and server details.
     */
    public String getKeyRangeRead() {
        StringBuilder builder = new StringBuilder();

        Node node = head;

        if (getSize() > 2) {
            do {
                builder.append(node.getPrev().getPrev().getStartRange());
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

        return toString();
    }

    /**
     * Converts the ring list into a formatted string representation.
     *
     * @return A string representation of the ring list or null if the list is empty.
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

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Parses and updates the metadata for the ring list based on the provided data string.
     *
     * @param data The metadata string to parse and apply.
     */
    public void parseAndUpdateMetaData(String data) {
        if (data.equals("null")) {
            return;
        }
        head = null;
        size = 0;
        String[] splitted = data.split(";");
        for (String valueString : splitted) {
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

        public Node(String IP, String port, String endRange) {
            this.prev = this;
            this.next = this;
            this.IP = IP;
            this.port = port;
            this.startRange = endRange;
            this.endRange = endRange;
        }

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


        /**
         * toString for formating the nodes
         *
         * @return String in the format IP|PORT|STARTRANGE|ENDRANGE
         */
        @Override
        public String toString() {
            return getIP() + "|" + getPort() + "|" + getStartRange() + "|" + getEndRange();
        }
    }


}
