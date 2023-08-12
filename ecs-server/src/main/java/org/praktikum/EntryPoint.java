package org.praktikum;

import java.util.Arrays;
import java.util.logging.Level;

public class EntryPoint {
    /**
     * Sets the flag value based on the provided flag.
     *
     * @param flags An array of flag values.
     * @param flag  The flag to be set.
     * @param value The value to assign to the flag.
     */
    private static void setFlag(String[] flags, String flag, String value) {
        switch (flag) {
            case "-p" -> flags[0] = value;
            case "-a" -> flags[1] = value;
            case "-l" -> flags[2] = value;
            case "-ll" -> flags[3] = value;
            default -> printInvalidInput(flags);
        }
    }

    /**
     * Prints the help text showing how to use the application's arguments.
     */
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
                "-l", "Sets the path for the log files on the server",
                "-l <path>",
                "-l /logs"
        );
        printArgumentHelp(
                "-ll", "Sets the loglevel",
                "-ll <loglevel>",
                "-ll INFO"
        );
    }

    /**
     * Prints the description, usage, example, and default value for a given command argument.
     *
     * @param command      The command argument.
     * @param description  A brief description of the command.
     * @param usage        How to use the command.
     * @param example      An example usage of the command.
     * @param defaultValue The default values for the command.
     */
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

    /**
     * Prints an error message indicating that the input provided by the user is invalid.
     *
     * @param flags The flags provided by the user.
     */
    public static void printInvalidInput(String[] flags) {
        System.out.println("You provided invalid input! Please refer to the help page.");
        System.out.println(Arrays.toString(flags));
    }

    public static void main(String[] args) {
        System.out.println("provided args: " + Arrays.toString(args));
        String[] flags = new String[4];
        flags[1] = "127.0.0.1";
        flags[2] = "logs/ecs.log";
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

        if (flags[0] == null || flags[1] == null || flags[2] == null || flags[3] == null) {
            printInvalidInput(flags);
            return;
        }

        try {
            int port = Integer.parseInt(flags[0]);
            Level logLevel = Level.parse(flags[3]);
            ECSServer ECS = new ECSServer(port, flags[1], flags[2], logLevel);
            ECS.runServer();
        } catch (Exception e) {
            System.out.println("An error occurred while starting up the server");
            System.out.println(e.getMessage());
        }
    }
}
