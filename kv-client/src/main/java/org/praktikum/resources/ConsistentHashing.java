package org.praktikum.resources;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ConsistentHashing {

    private MessageDigest digest;

    public ConsistentHashing() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("MD5");
    }
    /**
     * Computes the MD5 hash of the given key.
     *
     * @param key The string for which to compute the MD5 hash.
     * @return The MD5 hash of the input key as a 32-character hexadecimal string.
     */
    public String getMD5Hash(String key) {
        byte[] hash = this.digest.digest(key.getBytes());
        BigInteger bigInteger = new BigInteger(1, hash);
        return String.format("%032X",bigInteger);
    }
}
