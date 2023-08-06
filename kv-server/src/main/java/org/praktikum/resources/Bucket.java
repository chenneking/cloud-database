package org.praktikum.resources;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Bucket {
    String startRange;
    String endRange;
    String size;

    public ArrayList<String> getBucketList() {
        return bucketList;
    }

    private ArrayList<String> bucketList = new ArrayList<>();

    public Bucket(String startRange, String endRange){
        this.startRange = startRange;
        this.endRange = endRange;
    }

    public boolean insert(String key, String hash){
        if(isRightBucket(hash)){
            // Case Update: don't store keys twice, when value is updated.
            bucketList.remove(key);
            return bucketList.add(key);
        }
        return false;
    }

    public boolean delete(String key, String hash){
        if(isRightBucket(hash)){
            return bucketList.remove(key);
        }
        return false;
    }

    public int size(){
        return bucketList.size();
    }

    public String getStartRange() {
        return startRange;
    }

    public String getEndRange() {
        return endRange;
    }

    private boolean isRightBucket(String hash){
        if (getStartRange().compareTo(hash) < 0 && getEndRange().compareTo(hash) >= 0) {
            return true;
        }
        else if (getStartRange().compareTo(getEndRange()) >= 0) {
            if (getStartRange().compareTo(hash) < 0 && getEndRange().compareTo(hash) <= 0) {
                return true;
            }
            else if (getStartRange().compareTo(hash) > 0 && getEndRange().compareTo(hash) >= 0) {
                return true;
            }
        }
        return false;
    }
}
