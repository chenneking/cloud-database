package org.praktikum.resources;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FrequencyTable {
    private int numberOfBuckets;
    private final int offloadThreshold;

    public void setTotalBucketSize(int totalBucketSize) {
        this.totalBucketSize = totalBucketSize;
    }

    private int totalBucketSize = 0;
    private static final BigInteger MAX_VALUE = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    private static final BigInteger ZERO = new BigInteger("0", 16);
    private static final int NUMBER_OF_KEYRANGE_CHARS_TO_INCLUDE_IN_PRINT = 3;
    private final ConsistentHashing hashing;

    //each array list represents the bucket for the keyRange saved in the String
    private ArrayList<Bucket> buckets;
    public FrequencyTable(int numberOfBuckets, int offloadThreshold) {
        this.numberOfBuckets = numberOfBuckets;
        this.offloadThreshold = offloadThreshold;
        if(offloadThreshold > 50 || offloadThreshold < 0){
            throw new NumberFormatException();
        }
        try {
            hashing = new ConsistentHashing();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    public ArrayList<Bucket> getBuckets() {
        return buckets;
    }
    public int getNumberOfBuckets() {
        return numberOfBuckets;
    }
    public void updateBuckets(String startKeyRange, String endKeyRange) {
        if (buckets == null || buckets.size() == 0) {
            createBuckets(startKeyRange, endKeyRange);
        } else {
            if (! startKeyRange.equals(buckets.get(0).getStartRange()) || ! endKeyRange.equals(buckets.get(buckets.size() - 1).getEndRange()) || buckets.size() < numberOfBuckets) {
                List<String> allKeys = new LinkedList<>();
                for (Bucket bucket : buckets) {
                    allKeys.addAll(bucket.getBucketList());
                }
                createBuckets(startKeyRange, endKeyRange);
                for (String key : allKeys) {
                    addToTable(key, hashing.getMD5Hash(key));
                }
            }
        }
    }
    public void createBuckets(String startKeyRange, String endKeyRange){
        BigInteger startRange = new BigInteger(startKeyRange, 16);
        BigInteger endRange = new BigInteger(endKeyRange, 16);

        //Calculate width of key range to be divided up. Handles wrap-around case (e.g. FA... to 21... ranges)
        BigInteger totalRange;
        if (startRange.compareTo(endRange) > 0) {
            totalRange = MAX_VALUE.subtract(startRange).add(BigInteger.ONE).add(endRange);
        }
        else if (startRange.equals(endRange)) {
            totalRange = MAX_VALUE.add(BigInteger.ONE);
        }
        else {
            totalRange = endRange.subtract(startRange);
        }

        //if there are fewer values in the range than requested amount of buckets, we reduce the bucket count.
        if (totalRange.compareTo(BigInteger.valueOf(numberOfBuckets)) < 0){
            numberOfBuckets = totalRange.intValue();
        }


        BigInteger bucketSize = totalRange.divide(BigInteger.valueOf(numberOfBuckets));
        // Calculate the remainder so that it can be split among the first buckets, if we can't split the keyrange into buckets of identical size.
        BigInteger remainder = totalRange.mod(BigInteger.valueOf(numberOfBuckets));

        buckets = new ArrayList<>(numberOfBuckets);
        totalBucketSize = 0;

        for (int i = 0; i < numberOfBuckets; i++) {
            BigInteger bucketEndRange = startRange.add(bucketSize);
            if (i < remainder.intValue()){
                bucketEndRange = bucketEndRange.add(BigInteger.ONE);
            }
            // Wrap around case
            if (bucketEndRange.compareTo(MAX_VALUE) > 0) {
                bucketEndRange = bucketEndRange.subtract(MAX_VALUE).subtract(BigInteger.ONE);
            }
            buckets.add(new Bucket(String.format("%032X", startRange), String.format("%032X", bucketEndRange)));
            startRange = bucketEndRange;
            if (startRange.compareTo(MAX_VALUE) > 0) {
                startRange = startRange.subtract(MAX_VALUE);
            }
        }
    }
    public void addDummyBucket(String data){
        String[] split = data.trim().split(";");

        String dummyStartAndEnd = buckets.get(0).getStartRange();
        Bucket dummyBucket = new Bucket(dummyStartAndEnd, dummyStartAndEnd);
        for (String str : split) {
            String[] key_val_arr = str.split(",");
            dummyBucket.getBucketList().add(key_val_arr[0]);
        }
        buckets.add(dummyBucket);
    }
    public void addToTable(String key, String hash) {
        totalBucketSize += 1;
        for (Bucket bucket : buckets) {
            bucket.insert(key, hash);
        }
    }
    public void deleteFromTable(String key, String hash){
        totalBucketSize -= 1;
        for (Bucket bucket : buckets) {
            bucket.delete(key, hash);
        }
    }
    // lower: von links, nicht lower: von rechts
    public String [] calculateOffloadKeyRange(boolean lower) {
        String startRange;
        String endRange;

        if(lower){
            int bucketIndex = 0;
            startRange = buckets.get(0).getStartRange();
            int cumulativeBucketSize = 0;
            for (Bucket bucket : buckets) {
                cumulativeBucketSize += bucket.size();
                double percentage = cumulativeBucketSize == 0 ? 0 : BigInteger.valueOf(cumulativeBucketSize).doubleValue() / BigInteger.valueOf(totalBucketSize).doubleValue() * 100;
                if (percentage >= offloadThreshold) {
                    break;
                }
                if(bucketIndex < buckets.size()-1){
                    bucketIndex++;
                }
            }
            endRange = buckets.get(bucketIndex).getEndRange();
            if (bucketIndex >= 0) {
                if(buckets.subList(0, bucketIndex + 1).size() == buckets.size()){
                    return new String[]{startRange, startRange};
                }
                buckets.subList(0, bucketIndex + 1).clear();
            }
            return new String[]{startRange, endRange};
        }
        else{
            int bucketIndex = buckets.size() - 1;
            startRange = buckets.get(buckets.size()-1).getEndRange();
            int cumulativeBucketSize = 0;
            for(int j = buckets.size()-1; j >= 0; j--){
                cumulativeBucketSize += buckets.get(j).size();
                double percentage = cumulativeBucketSize == 0 ? 0 : BigInteger.valueOf(cumulativeBucketSize).doubleValue() / BigInteger.valueOf(totalBucketSize).doubleValue() * 100;
                if(percentage >= offloadThreshold){
                    break;
                }
                if(bucketIndex > 0){
                    bucketIndex --;
                }
            }
            endRange = buckets.get(bucketIndex).getStartRange();
            if (bucketIndex >= 0) {
                if(buckets.subList(bucketIndex, buckets.size()).size() == buckets.size()){
                    return new String[]{endRange, endRange};
                }
                buckets.subList(bucketIndex, buckets.size()).clear();
            }
            return new String[]{endRange, startRange};
        }

    }
    public void removeBucket(int countToRemove, boolean lower){
        if(lower){
            if (countToRemove > 0) {
                buckets.subList(0, countToRemove).clear();
            }
        }
        else{
            if (countToRemove > 0) {
                buckets.subList(0, buckets.size()-countToRemove).clear();
            }
        }
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (buckets.size() == 0) {
            return "No Buckets present.";
        }

        for (Bucket bucket : buckets) {
            int bucketSize = bucket.size();

            double percentage = bucketSize == 0 ? 0 : BigInteger.valueOf(bucketSize).doubleValue() / BigInteger.valueOf(totalBucketSize).doubleValue() * 100;
            int numHashes = (int) Math.round(percentage);
            String hashes = "#".repeat(numHashes);

            builder.append(bucket.getStartRange(), 0, NUMBER_OF_KEYRANGE_CHARS_TO_INCLUDE_IN_PRINT);
            builder.append(" to ");
            builder.append(bucket.getEndRange(), 0, NUMBER_OF_KEYRANGE_CHARS_TO_INCLUDE_IN_PRINT);
            builder.append(" : ");
            builder.append(hashes);
            if (numHashes > 0) {
                builder.append(" ");
            }
            builder.append(String.format("%.2f", percentage));
            builder.append("%");
            builder.append("\n");
        }
        builder.append("Where each # represents 1% of the keys within the server");
        return builder.toString();
    }

    public String getAllInfo() {
        StringBuilder builder = new StringBuilder();

        for (Bucket bucket : buckets) {
            builder.append(bucket.getStartRange());
            builder.append("\n");
            builder.append(bucket.getEndRange());
            builder.append("\n");
            builder.append("----");
            builder.append("\n");
        }
        return builder.toString();
    }
}
