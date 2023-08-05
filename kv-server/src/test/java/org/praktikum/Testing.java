package org.praktikum;

import org.junit.jupiter.api.Test;
import org.praktikum.resources.Bucket;
import org.praktikum.resources.ConsistentHashing;
import org.praktikum.resources.FrequencyTable;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class Testing {

    ConsistentHashing hashing = new ConsistentHashing();

    public Testing() throws NoSuchAlgorithmException {
    }

    public void test1() {

        KVServer kv1 = new KVServer(8001,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server1.log", Level.ALL,100,"FIFO",0,0);
        KVServer kv2 = new KVServer(8002,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server2.log", Level.ALL,100,"FIFO",0,0);
        KVServer kv3 = new KVServer(8003,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server3.log", Level.ALL,100,"FIFO",0,0);
        KVServer kv4 = new KVServer(8004,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server4.log", Level.ALL,100,"FIFO",0,0);
        KVServer kv5 = new KVServer(8004,"127.0.0.1","127.0.0.1:9999","/Users/carl/IdeaProjects/Milestone3/data","/Users/carl/IdeaProjects/Milestone3/kv-server/logs/server5.log", Level.ALL,100,"FIFO",0,0);

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

    @Test
    public void testBucketCreationStandard() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        assertNull(frequencyTable.getBuckets());
        frequencyTable.createBuckets("1000000000000000","6000000000000000");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals(createdBuckets.get(i).getStartRange(), BigInteger.valueOf(i + 1).toString(16) + "000000000000000");
            assertEquals(createdBuckets.get(i).getEndRange(), BigInteger.valueOf(i + 2).toString(16) + "000000000000000");
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testBucketCreationWrapAround() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        assertNull(frequencyTable.getBuckets());
        frequencyTable.createBuckets("F000000000000000","4000000000000000");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals(createdBuckets.get(i).getStartRange(), BigInteger.valueOf(i + 15).mod(BigInteger.valueOf(16)).toString(16).toUpperCase() + "000000000000000");
            assertEquals(createdBuckets.get(i).getEndRange(), BigInteger.valueOf(i + 15 + 1).mod(BigInteger.valueOf(16)).toString(16).toUpperCase() + "000000000000000");
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testBucketCreationTooManyBucketsForKeyRange() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        assertNull(frequencyTable.getBuckets());
        // we can only create 3 buckets out of these
        frequencyTable.createBuckets("00000045 0000000000","0000000000000003");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        assertEquals(3, createdBuckets.size());
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals( "000000000000000" + BigInteger.valueOf(i).toString(16).toUpperCase(), createdBuckets.get(i).getStartRange());
            assertEquals("000000000000000" + BigInteger.valueOf(i+ 1).toString(16).toUpperCase(), createdBuckets.get(i).getEndRange());
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testFrequencyTablePrint() {

    }

    @Test
    public void testInsert1() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        frequencyTable.createBuckets("1000000000000000","6000000000000000");
        frequencyTable.addToTable("ab", hashing.getMD5Hash("ab"));
        System.out.println(frequencyTable);
    }

    @Test
    public void danaiSpielHierRum() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        frequencyTable.createBuckets("0000000000000000","4000000000000000");
        System.out.println(frequencyTable);
    }
}

