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
    
    @Test
    public void testBucketCreationStandard() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        assertNull(frequencyTable.getBuckets());
        frequencyTable.createBuckets("10000000000000000000000000000000","60000000000000000000000000000000");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals(createdBuckets.get(i).getStartRange(), BigInteger.valueOf(i + 1).toString(16) + "0000000000000000000000000000000");
            assertEquals(createdBuckets.get(i).getEndRange(), BigInteger.valueOf(i + 2).toString(16) + "0000000000000000000000000000000");
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testBucketCreationWrapAround() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        assertNull(frequencyTable.getBuckets());
        frequencyTable.createBuckets("F0000000000000000000000000000000","40000000000000000000000000000000");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals(createdBuckets.get(i).getStartRange(), BigInteger.valueOf(i + 15).mod(BigInteger.valueOf(16)).toString(16).toUpperCase() + "0000000000000000000000000000000");
            assertEquals(createdBuckets.get(i).getEndRange(), BigInteger.valueOf(i + 15 + 1).mod(BigInteger.valueOf(16)).toString(16).toUpperCase() + "0000000000000000000000000000000");
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testBucketCreationTooManyBucketsForKeyRange() {
        FrequencyTable frequencyTable = new FrequencyTable(3, 50);
        assertNull(frequencyTable.getBuckets());
        // we can only create 3 buckets out of these
        frequencyTable.createBuckets("00000000000000000000000000000000","00000000000000000000000000000003");
        ArrayList<Bucket> createdBuckets = frequencyTable.getBuckets();
        assertEquals(3, createdBuckets.size());
        for (int i = 0; i < createdBuckets.size(); i++) {
            assertEquals( "0000000000000000000000000000000" + BigInteger.valueOf(i).toString(16).toUpperCase(), createdBuckets.get(i).getStartRange());
            assertEquals("0000000000000000000000000000000" + BigInteger.valueOf(i+ 1).toString(16).toUpperCase(), createdBuckets.get(i).getEndRange());
        }
        System.out.println(frequencyTable);
    }

    @Test
    public void testFrequencyTablePrint() {

    }

    @Test
    public void testInsert1() {
        FrequencyTable frequencyTable = new FrequencyTable(5, 50);
        frequencyTable.createBuckets("10000000000000000000000000000000","60000000000000000000000000000000");
        frequencyTable.addToTable("ab", hashing.getMD5Hash("ab"));
        System.out.println(frequencyTable);
    }

    @Test
    public void danaiSpielHierRum() {
        FrequencyTable frequencyTable = new FrequencyTable(9, 50);
        frequencyTable.createBuckets("000B97B0B6E75689CF143CC626655902","600007FFA927B1B1E2CB7E9A7DA8A273");
        System.out.println(frequencyTable.getAllInfo());
    }

    @Test
    public void testRangeCalculationLeft() {
        FrequencyTable frequencyTable = new FrequencyTable(3,20);
        frequencyTable.createBuckets("10000000000000000000000000000000","40000000000000000000000000000000");
        List<Bucket> bucketList = frequencyTable.getBuckets();
        bucketList.get(0).getBucketList().add("bucket 1");
        bucketList.get(1).getBucketList().add("bucket 2");
        bucketList.get(2).getBucketList().add("bucket 3");
        frequencyTable.setTotalBucketSize(3);
        String[] calculatedRanges = frequencyTable.calculateOffloadKeyRange(true);
        assertEquals("10000000000000000000000000000000", calculatedRanges[0]);
        assertEquals("20000000000000000000000000000000", calculatedRanges[1]);
    }

    @Test
    public void testRangeCalculationRight() {
        FrequencyTable frequencyTable = new FrequencyTable(3,20);
        frequencyTable.createBuckets("10000000000000000000000000000000","40000000000000000000000000000000");
        List<Bucket> bucketList = frequencyTable.getBuckets();
        frequencyTable.setTotalBucketSize(3);
        bucketList.get(0).getBucketList().add("bucket 1");
        bucketList.get(1).getBucketList().add("bucket 2");
        bucketList.get(2).getBucketList().add("bucket 3");
        String[] calculatedRanges = frequencyTable.calculateOffloadKeyRange(false);
        assertEquals("30000000000000000000000000000000", calculatedRanges[0]);
        assertEquals("40000000000000000000000000000000", calculatedRanges[1]);
    }
}

