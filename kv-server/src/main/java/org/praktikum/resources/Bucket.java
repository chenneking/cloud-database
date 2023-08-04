package org.praktikum.resources;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Bucket {
    String startRange;
    String endRange;
    String size;

    private ArrayList<String> bucketList = new ArrayList<>();

    public Bucket(String startRange, String endRange){
        this.startRange = startRange;
        this.endRange = endRange;
    }

    public boolean insert(String key, String hash){
        if(isRightBucket(hash)){
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

    //@ToDo implement logic
    private boolean isRightBucket(String hash){
        return true;
    }
}
