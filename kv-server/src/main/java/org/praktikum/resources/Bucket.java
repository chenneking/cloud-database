package org.praktikum.resources;

import java.util.ArrayList;

public class Bucket {
    String startRange;
    String endRange;
    String size;

    public ArrayList<String> getBucketList() {
        return bucketList;
    }

    private ArrayList<String> bucketList = new ArrayList<>();

    public Bucket(String startRange, String endRange) {
        this.startRange = startRange;
        this.endRange = endRange;
    }

    /**
     * Inserts a key into the bucket if it belongs to the right bucket based on its hash.
     *
     * @param key  The key to be inserted.
     * @param hash The hash value of the key.
     * @return true if the key is inserted successfully, false otherwise.
     */
    public boolean insert(String key, String hash) {
        if (isRightBucket(hash)) {
            // Case Update: don't store keys twice, when value is updated.
            bucketList.remove(key);
            return bucketList.add(key);
        }
        return false;
    }

    /**
     * Deletes a key from the bucket if it belongs to the right bucket based on its hash.
     *
     * @param key  The key to be deleted.
     * @param hash The hash value of the key.
     * @return true if the key is deleted successfully, false otherwise.
     */
    public boolean delete(String key, String hash) {
        if (isRightBucket(hash)) {
            return bucketList.remove(key);
        }
        return false;
    }

    public int size() {
        return bucketList.size();
    }

    public String getStartRange() {
        return startRange;
    }

    public String getEndRange() {
        return endRange;
    }

    /**
     * Checks if a given hash belongs to this bucket based on the start and end ranges.
     *
     * @param hash The hash value to be checked.
     * @return true if the hash belongs to this bucket, false otherwise.
     */
    private boolean isRightBucket(String hash) {
        if (getStartRange().compareTo(hash) < 0 && getEndRange().compareTo(hash) >= 0) {
            return true;
        } else if (getStartRange().compareTo(getEndRange()) >= 0) {
            if (getStartRange().compareTo(hash) < 0 && getEndRange().compareTo(hash) <= 0) {
                return true;
            } else if (getStartRange().compareTo(hash) > 0 && getEndRange().compareTo(hash) >= 0) {
                return true;
            }
        }
        return false;
    }
}
