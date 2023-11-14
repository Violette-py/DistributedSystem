import impl.ClientImpl;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ClientLauncher {

    private static final String CMD_OPEN = "open";
    private static final String CMD_READ = "read";
    private static final String CMD_APPEND = "append";
    private static final String CMD_CLOSE = "close";
    private static final String CMD_EXIT = "exit";

    private static final String INFO_FD = "INFO: fd=";
    private static final String INFO_READ_NOT_ALLOWED = "INFO: READ not allowed";
    private static final String INFO_WRITE_DONE = "INFO: write done";
    private static final String INFO_FD_CLOSED = "INFO: fd ";

    private ClientImpl client;

    public ClientLauncher(ClientImpl client) {
        this.client = client;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print(">> ");
            String input = scanner.nextLine();
            parseCommand(input);
        }
    }

    private void parseCommand(String input) {
        String[] tokens = input.split("\\s+");
        if (tokens.length == 0) {
            return;
        }

        String command = tokens[0];

        switch (command) {
            case CMD_OPEN:
                if (tokens.length >= 3) {
                    String filename = tokens[1];
                    int mode = Integer.parseInt(tokens[2]);
                    client.open(filename, mode);
                } else {
                    printUsage();
                }
                break;

            case CMD_READ:
                if (tokens.length >= 2) {
                    int fd = Integer.parseInt(tokens[1]);
                    client.read(fd);
                } else {
                    printUsage();
                }
                break;

            case CMD_APPEND:
                if (tokens.length >= 3) {
                    int fd = Integer.parseInt(tokens[1]);
                    String content = input.substring(input.indexOf(tokens[2]));
                    client.append(fd, content.getBytes(StandardCharsets.UTF_8));
                } else {
                    printUsage();
                }
                break;

            case CMD_CLOSE:
                if (tokens.length >= 2) {
                    int fd = Integer.parseInt(tokens[1]);
                    client.close(fd);
                } else {
                    printUsage();
                }
                break;

            case CMD_EXIT:
                exit();
                break;

            default:
                System.out.println("Unknown command. Type 'exit' to quit.");
        }
    }


    private void exit() {
        System.out.println("INFO: bye");
        System.exit(0);
    }

    private void printUsage() {
        System.out.println("Usage: open <filename> <mode> | read <fd> | append <fd> <content> | close <fd> | exit");
    }

    public static void main(String[] args) {
        // Assuming you have a ClientImpl instance
        ClientImpl clientImpl = new ClientImpl();
        ClientLauncher clientLauncher = new ClientLauncher(clientImpl);
        clientLauncher.start();
    }
}