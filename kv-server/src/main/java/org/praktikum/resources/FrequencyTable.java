package org.praktikum.resources;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

public class FrequencyTable {
    private int numberOfBuckets;
    private final int offloadThreshold;
    private int totalBucketSize = 0;
    private static final BigInteger MAX_VALUE = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    private static final BigInteger ZERO = new BigInteger("0", 16);
    private static final int NUMBER_OF_KEYRANGE_CHARS_TO_INCLUDE_IN_PRINT = 3;

    //each array list represents the bucket for the keyRange saved in the String
    private ArrayList<Bucket> buckets;
    public FrequencyTable(int numberOfBuckets, int offloadThreshold) {
        this.numberOfBuckets = numberOfBuckets;
        this.offloadThreshold = offloadThreshold;
        if(offloadThreshold > 50 || offloadThreshold < 0){
            throw new NumberFormatException();
        }
    }
    public ArrayList<Bucket> getBuckets() {
        return buckets;
    }
    public int getNumberOfBuckets() {
        return numberOfBuckets;
    }
    public void createBuckets(String startKeyRange, String endKeyRange){
        BigInteger startRange = new BigInteger(startKeyRange, 16);
        BigInteger endRange = new BigInteger(endKeyRange, 16);

        //Calculate width of key range to be divided up. Handles wrap-around case (e.g. FA... to 21... ranges)
        BigInteger totalRange;
        if (startRange.compareTo(endRange) > 0) {
            totalRange = MAX_VALUE.subtract(startRange).add(BigInteger.ONE).add(endRange);
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
        int bucketIndex = 0;
        if(lower){
            startRange = buckets.get(0).getStartRange();
            for (Bucket bucket : buckets) {
                if ((bucket.size() / totalBucketSize) * 100 >= offloadThreshold) {
                    break;
                }
                if(bucketIndex < buckets.size()-1){
                    bucketIndex++;
                }

            }
            endRange = buckets.get(bucketIndex).getEndRange();
        }
        else{
            startRange = buckets.get(buckets.size() -1).getEndRange();
            for(int j = buckets.size()-1; j >= 0; j--){
                if((buckets.get(j).size()/totalBucketSize) * 100 >= offloadThreshold){
                    break;
                }
                if(bucketIndex < buckets.size()-1){
                    bucketIndex++;
                }

            }
            endRange = buckets.get(bucketIndex).getStartRange();
        }
        return new String[]{startRange, endRange};
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
    // TODO: this method should return ascii string of pretty-printed frequency table that can be sent to kv-client
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
