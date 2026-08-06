package com.github.nicolasholanda.mq.cli;

import java.io.IOException;
import java.util.Arrays;

public final class MqCli {

    private MqCli() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
            return;
        }
        String command = args[0];
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (command) {
            case "dump-log" -> DumpLog.main(rest);
            default -> {
                System.out.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
            }
        }
    }

    private static void printUsage() {
        System.out.println("usage: mq <command> [options]");
        System.out.println();
        System.out.println("commands:");
        System.out.println("  dump-log    inspect the batches and records of a segment file");
        DumpLog.printUsage(System.out);
    }
}
