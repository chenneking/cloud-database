package org.praktikum;

import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Level;

public class EntryPoint {

    private static void setFlag(String[] flags, String flag, String value) {
        switch (flag) {
            case "-p" -> flags[0] = value;
            case "-a" -> flags[1] = value;
            case "-b" -> flags[2] = value;
            case "-d" -> flags[3] = value;
            case "-l" -> flags[4] = value;
            case "-ll" -> flags[5] = value;
            case "-c" -> flags[6] = value;
            case "-s" -> flags[7] = value;
            case "-bc" -> flags[8] = value;
            case "-t" -> flags[9] = value;
            case "-e" -> flags[10] = value;
            case "-h" -> printHelpText();
            default -> printInvalidInput(flags);
        }
    }

    public static void printHelpText() {
        System.out.println("Arguments:");
        printArgumentHelp(
                "-p",
                "Sets the port of the server",
                "-p <port>",
                "-p 8080"
        );
        printArgumentHelp(
                "-a",
                "Sets the listening address of the server",
                "-a <address>",
                "-a 123.123.123.1",
                "127.0.0.1"
        );
        printArgumentHelp(
                "-b",
                "Sets the address of the boostrap server",
                "-b <address>",
                "-b 123.123.123.1"
        );
        printArgumentHelp(
                "-d",
                "Sets the directory for persistent storage files on the server",
                "-d <directory>",
                "-d /data"
        );
        printArgumentHelp(
                "-l",
                "Sets the path for the log files on the server",
                "-l <path>",
                "-l /logs"
        );
        printArgumentHelp(
                "-ll",
                "Sets the loglevel",
                "-ll <loglevel>",
                "-ll INFO"
        );
        printArgumentHelp(
                "-c",
                "Sets the cache size in number of keys",
                "-c <size>",
                "-c 100"
        );
        printArgumentHelp(
                "-s",
                "Sets the cache displacement strategy. Options are: FIFO / LFU / LRU.",
                "-s <strategy>",
                "-s FIFO"
        );
        printArgumentHelp(
                "-bc",
                "Sets the number of buckets that the keyrange of a KVServer is split up into",
                "-bc <count>",
                "-bc 4",
                "3"
        );
        printArgumentHelp(
                "-t",
                "Sets the offload threshold value for a keyrange transfer between KVServers in percent",
                "-t <threshold>",
                "-t 33",
                "34"
        );
        printArgumentHelp(
                "-e",
                "Provide a 32 character long customized endRange of the KVServer. Has to be within The range of 00 - FF.",
                "-e <endRange>",
                "-e 60000000000000000000000000000000"
        );
        //TODO: HIER OPTIMA IN DEFAULT VALUES UPDATEN
    }

    private static void printArgumentHelp(String command, String description, String usage, String example, String... defaultValue) {
        System.out.format("\n%-12s%s", command, description);
        if (usage != null) {
            System.out.format("\n\t\t\t%s", "Usage:      " + usage);
            System.out.format("\n\t\t\t%s", "Example:    " + example);
            if (defaultValue.length > 0) {
                System.out.format("\n\t\t\t%s", "Default value: " + String.join("\n\t\t\t            ", defaultValue));
            }
        }
        System.out.println();
    }

    public static void printInvalidInput(String[] flags) {
        System.out.println("You provided invalid input! Please refer to the help page.");
        System.out.println(Arrays.toString(flags));
    }

    public static void main(String[] args) {
        System.out.println("provided args: " + Arrays.toString(args));
        String[] flags = new String[11];
        //Set default value for address
        flags[1] = "127.0.0.1";
        flags[4] = "logs/server.log";

        //TODO: DEFAULT WERTE FÜR BUCKET COUNT UND THRESHOLD WERTE AUF OPTIMA SETZEN
        flags[8] = "3";
        flags[9] = "34";
        //Default for -e flag: empty string.
        flags[10] = "";

        //Parse CLI parameters
        for (int i = 0; i < args.length; i++) {
            String s = args[i];
            //check if it's a flag
            if (s.startsWith("-")) {
                try {
                    setFlag(flags, args[i], args[i + 1]);
                } catch (IndexOutOfBoundsException e) {
                    printHelpText();
                    return;
                }
            }
        }

        if (flags[0] == null || flags[3] == null || flags[4] == null || flags[5] == null || flags[6] == null || flags[7] == null) {
            printInvalidInput(flags);
            return;
        }

        try {
            int port = Integer.parseInt(flags[0]);
            int cacheSize = Integer.parseInt(flags[6]);
            Level logLevel = Level.parse(flags[5]);
            int numberOfBuckets = Integer.parseInt(flags[8]);
            int offloadThreshold = Integer.parseInt(flags[9]);
            KVServer KVServer = new KVServer(port, flags[1], flags[2], flags[3], flags[4], logLevel, cacheSize, flags[7], numberOfBuckets, offloadThreshold, flags[10]);
            KVServer.runServer();
        } catch (Exception e) {
            System.out.println("An error occurred while starting up the server");
            System.out.println(e.getMessage());
        }
    }
}
