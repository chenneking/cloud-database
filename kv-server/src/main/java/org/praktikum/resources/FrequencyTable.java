package org.praktikum.resources;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class FrequencyTable {
    private int numberOfBuckets;
    private int offloadThreshold;
    private int totalBucketSize = 0;
    //each array list represents the bucket for the keyRange saved in the String
    private ArrayList<Bucket> buckets;
    public FrequencyTable(int numberOfBuckets, int offloadThreshold) {
        this.numberOfBuckets = numberOfBuckets;
        this.offloadThreshold = offloadThreshold;
        if(offloadThreshold > 100 || offloadThreshold < 0){
            throw new NumberFormatException();
        }
    }
    public void createBuckets(String keyRange){
        buckets = new ArrayList<>();
        // ToDO: implement splitting logic of the client here
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
                bucketIndex++;
            }
            endRange = buckets.get(bucketIndex).getEndRange();
        }
        else{
            startRange = buckets.get(buckets.size() -1).getEndRange();
            for(int j = buckets.size()-1; j >= 0; j--){
                if((buckets.get(j).size()/totalBucketSize) * 100 >= offloadThreshold){
                    break;
                }
                bucketIndex++;
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
        return "" +totalBucketSize;
    }
}
