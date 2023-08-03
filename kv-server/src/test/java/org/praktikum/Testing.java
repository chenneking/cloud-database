package org.praktikum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

public class Testing {
    public static void main(String[] args) {
        Testing t = new Testing();
        t.test1();
    }

    public void test1() {

        KVServer kv1 = new KVServer(8001,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server1.log", Level.ALL,100,"FIFO");
        KVServer kv2 = new KVServer(8002,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server2.log", Level.ALL,100,"FIFO");
        KVServer kv3 = new KVServer(8003,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server3.log", Level.ALL,100,"FIFO");
        KVServer kv4 = new KVServer(8004,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server4.log", Level.ALL,100,"FIFO");
        KVServer kv5 = new KVServer(8004,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server5.log", Level.ALL,100,"FIFO");

        KVServer[] kvServers = new KVServer[]{kv1, kv2, kv3, kv4, kv5};

        List<Integer> numList = new ArrayList<>();
        numList.add(1);
        numList.add(2);
        numList.add(3);
        numList.add(4);

        Collections.shuffle(numList);

        for (KVServer server : kvServers) {
            server.runServer();
        }

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (Integer i : numList) {
            System.out.println("Shutting down KVServer " + i);
            //TODO logic here
        }
    }
}
