import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ConsistentHashing {

    private final MessageDigest digest;

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
        return String.format("%032X", bigInteger);
    }

    /**
     * Computes the MD5 hash of the IP and port combination.
     *
     * @param IP   The IP address as a string.
     * @param port The port as a string.
     * @return The MD5 hash of the IP and port combination as a 32-character hexadecimal string.
     */
    public String getMD5Hash(String IP, String port) {
        byte[] hash = this.digest.digest((IP + ":" + port).getBytes());
        BigInteger bigInteger = new BigInteger(1, hash);
        return String.format("%032X", bigInteger);
    }
}
