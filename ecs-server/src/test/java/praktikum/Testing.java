package praktikum;

import org.praktikum.resources.RingList;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

public class Testing {
    public static void main(String[] args) {
        Testing t = new Testing();
        t.test2();

    }

    public void test1() {
        try {
            RingList ringList = new RingList();

            RingList.Node node1 = ringList.add("127.0.0.1", "8001", null);
            System.out.println("Added: " + node1);
            System.out.println(ringList);
            RingList.Node node2 = ringList.add("127.0.0.1", "8002", null);
            System.out.println("Added: " + node2);
            System.out.println(ringList);
            RingList.Node node3 = ringList.add("127.0.0.1", "8003", null);
            System.out.println("Added: " + node3);
            System.out.println(ringList);
            RingList.Node node4 = ringList.add("127.0.0.1", "8004", null);
            System.out.println("Added: " + node4);
            System.out.println(ringList);
            RingList.Node node5 = ringList.add("127.0.0.1", "8005", null);
            System.out.println("Added: " + node5);
            System.out.println(ringList);

            RingList.Node rem1 = ringList.remove("127.0.0.1", "8003");
            System.out.println("Removed: " + rem1);
            System.out.println(ringList);
            RingList.Node rem2 = ringList.remove("127.0.0.1", "8002");
            System.out.println("Removed: " + rem2);
            System.out.println(ringList);
            RingList.Node rem3 = ringList.remove("127.0.0.1", "8005");
            System.out.println("Removed: " + rem3);
            System.out.println(ringList);
            RingList.Node rem4 = ringList.remove("127.0.0.1", "8004");
            System.out.println("Removed: " + rem4);
            System.out.println(ringList);
            RingList.Node rem5 = ringList.remove("127.0.0.1", "8001");
            System.out.println("Removed: " + rem5);
            System.out.println(ringList);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

    }

    public void test2() {
        RingList ringList = null;
        try {
            ringList = new RingList();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        ringList.add("12","1000", null);
        ringList.add("17","1000", null);
        ringList.add("19","1000", null);
        ringList.add("21", "1000", null);
        ringList.add("22", "1000", null);

        ringList.remove("19","1000");
        ringList.remove("17", "1000");
        ringList.remove("12", "1000");
        ringList.remove("22", "1000");
        System.out.println(ringList);
    }
}
