import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Generator {
    private ConsistentHashing consistentHashing;

    public Generator() {
        try {
            this.consistentHashing = new ConsistentHashing();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void findMS4IPPortCombos() {
        for (int i = 1024; i <= 65535; i++) {
            String hash = consistentHashing.getMD5Hash("127.0.0.1", Integer.toString(i));
            if (hash.startsWith("B00")){
                System.out.println("KOMBO FOUND: " + hash + " | " + i);
            }
        }
    }

    public void generateHashInRange(String startRange, String endRange, int n, String outputFile) {
        BigInteger start = new BigInteger(startRange, 16);
        BigInteger end = new BigInteger(endRange, 16);

        Map<String, String> validKeys = new HashMap<>();
        Random random = new Random();

        while (validKeys.size() < n) {
            String key = new BigInteger(130, random).toString(32);
            String hash = consistentHashing.getMD5Hash(key);

            BigInteger hashValue = new BigInteger(hash, 16);
            if (start.compareTo(hashValue) < 0 && end.compareTo(hashValue) >= 0) {
                validKeys.put(key, hash);
            }
            else if (start.compareTo(end) >= 0) {
                if (start.compareTo(hashValue) < 0 && end.compareTo(hashValue) <= 0) {
                    validKeys.put(key, hash);
                }
                else if (start.compareTo(hashValue) > 0 && end.compareTo(hashValue) >= 0) {
                    validKeys.put(key, hash);
                }
            }
        }
        try {
            writeToCSV(validKeys, outputFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeToCSV(Map<String, String> validKeys, String outputFile) throws IOException {
        try (
                BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile));
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Key", "Hash"))
        ) {
            for (Map.Entry<String, String> entry : validKeys.entrySet()) {
                csvPrinter.printRecord(entry.getKey(), entry.getValue());
            }
            csvPrinter.flush();
        }
    }

    public void generate3Buckets() {
        String folder = "/Users/carl/IdeaProjects/ms5/benchmarking/spikes/3_buckets";
        this.generateHashInRange(
                "000B97B0B6E75689CF143CC626655902",
                "2007BD205CFCCA41D5A6A80CEE26717D",
                1000,
                folder + "/bucket1.csv");
        this.generateHashInRange(
                "2007BD205CFCCA41D5A6A80CEE26717D",
                "4003E29003123DF9DC391353B5E789F8",
                8000,
                folder + "/bucket2.csv");
        this.generateHashInRange(
                "4003E29003123DF9DC391353B5E789F8",
                "600007FFA927B1B1E2CB7E9A7DA8A273",
                1000,
                folder + "/bucket3.csv");
    }
    public void generate6Buckets() {
        String folder = "/Users/carl/IdeaProjects/ms5/benchmarking/spikes/6_buckets";

        this.generateHashInRange(
                "000B97B0B6E75689CF143CC626655902",
                "1009AA6889F21065D25D72698A45E540",
                3000,
                folder + "/bucket1.csv");
        this.generateHashInRange(
                "1009AA6889F21065D25D72698A45E540",
                "2007BD205CFCCA41D5A6A80CEE26717E",
                500,
                folder + "/bucket2.csv");
        this.generateHashInRange(
                "2007BD205CFCCA41D5A6A80CEE26717E",
                "3005CFD83007841DD8EFDDB05206FDBC",
                2000,
                folder + "/bucket3.csv");
        this.generateHashInRange(
                "3005CFD83007841DD8EFDDB05206FDBC",
                "4003E29003123DF9DC391353B5E789F9",
                500,
                folder + "/bucket4.csv");
        this.generateHashInRange(
                "4003E29003123DF9DC391353B5E789F9",
                "5001F547D61CF7D5DF8248F719C81636",
                3500,
                folder + "/bucket5.csv");
        this.generateHashInRange(
                "5001F547D61CF7D5DF8248F719C81636",
                "600007FFA927B1B1E2CB7E9A7DA8A273",
                500,
                folder + "/bucket6.csv");
    }

    public void generate9Buckets() {
        String folder = "/Users/carl/IdeaProjects/ms5/benchmarking/spikes/9_buckets";

        this.generateHashInRange(
                "000B97B0B6E75689CF143CC626655902",
                "0AB4F98098EE7D1C7BEFB5DDBE50612B",
                250,
                folder + "/bucket1.csv");
        this.generateHashInRange(
                "0AB4F98098EE7D1C7BEFB5DDBE50612B",
                "155E5B507AF5A3AF28CB2EF5563B6954",
                500,
                folder + "/bucket2.csv");
        this.generateHashInRange(
                "155E5B507AF5A3AF28CB2EF5563B6954",
                "2007BD205CFCCA41D5A6A80CEE26717D",
                2500,
                folder + "/bucket3.csv");
        this.generateHashInRange(
                "2007BD205CFCCA41D5A6A80CEE26717D",
                "2AB11EF03F03F0D482822124861179A6",
                500,
                folder + "/bucket4.csv");
        this.generateHashInRange(
                "2AB11EF03F03F0D482822124861179A6",
                "355A80C0210B17672F5D9A3C1DFC81CF",
                2500,
                folder + "/bucket5.csv");
        this.generateHashInRange(
                "355A80C0210B17672F5D9A3C1DFC81CF",
                "4003E29003123DF9DC391353B5E789F8",
                750,
                folder + "/bucket6.csv");
        this.generateHashInRange(
                "4003E29003123DF9DC391353B5E789F8",
                "4AAD445FE519648C89148C6B4DD29221",
                2250,
                folder + "/bucket7.csv");
        this.generateHashInRange(
                "4AAD445FE519648C89148C6B4DD29221",
                "5556A62FC7208B1F35F00582E5BD9A4A",
                400,
                folder + "/bucket8.csv");
        this.generateHashInRange(
                "5556A62FC7208B1F35F00582E5BD9A4A",
                "600007FFA927B1B1E2CB7E9A7DA8A273",
                350,
                folder + "/bucket9.csv");
    }

    public static void main(String[] args) {
        Generator generator = new Generator();
        generator.generate3Buckets();
        generator.generate6Buckets();
        generator.generate9Buckets();
    }
}
