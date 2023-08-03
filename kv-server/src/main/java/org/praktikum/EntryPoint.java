package org.praktikum;

import java.util.Arrays;
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
            case "-h" -> printHelpText();

            //TODO: hier flag einfügen für die Anzahl an Buckets (zumindest initially)
            //TODO: hier flag einfügen für die Offload threshold

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
        //TODO: hier argument help einfügen für die neuen flags (number of buckets + threshold percentage)
        // falls wir default werte setzen wollen, müssen wir die hier auch beschreiben (evtl. default werte based auf unserem optimum)
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
        String[] flags = new String[8];
        //Set default value for address
        flags[1] = "127.0.0.1";
        flags[4] = "logs/server.log";

        //TODO: default werte hier setzen für die neuen flags, falls wir das so machen

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
            KVServer KVServer = new KVServer(port, flags[1], flags[2], flags[3], flags[4], logLevel, cacheSize, flags[7]);
            KVServer.runServer();
        } catch (Exception e) {
            System.out.println("An error occurred while starting up the server");
            System.out.println(e.getMessage());
        }
    }
}
