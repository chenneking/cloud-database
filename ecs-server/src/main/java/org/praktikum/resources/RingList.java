package org.praktikum.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RingList {
    Node head;
    int size = 0;

    private final MessageDigest digest;

    public RingList() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("MD5");
    }

    /**
     * Adds a node, with the IP and port given, to the ringlist
     *
     * @param IP
     * @param port
     * @return the new node
     */
    public synchronized Node add(String IP, String port) {
        String hashString = getMD5Hash(IP, port);

        Node previousNode = find(hashString);

        Node newNode;
        if (previousNode == null) {
            newNode = new Node(IP, port, hashString);
        }
        else {
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
        }
        else if (size == 1) {
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
        }
        else {
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
     * Removes a node with given IP and port form ringlist
     *
     * @param IP
     * @param port
     * @return the removed node
     */
    public synchronized Node remove(String IP, String port) {
        String hashString = getMD5Hash(IP, port);
        return remove(hashString);
    }

    /**
     * Finds the node where the given key fits within the start and end ranges.
     *
     * @param key hash key
     * @return The found node or null if non-existant
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
            }
            else if (node.getStartRange().compareTo(node.getEndRange()) >= 0) {
                if (node.getStartRange().compareTo(key) < 0 && node.getEndRange().compareTo(key) <= 0) {
                    return node;
                }
                else if (node.getStartRange().compareTo(key) > 0 && node.getEndRange().compareTo(key) >= 0) {
                    return node;
                }
            }
            node = node.getNext();
        } while (node != head);
        return null;
    }

    /**
     * Converts the ring list to a string representation.
     *
     * @return A string representation of the ring list, or null if the list is empty.
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

        @Override
        public String toString() {
            return getIP() + "|" + getPort() + "|" + getStartRange() + "|" + getEndRange();
        }
    }
}
