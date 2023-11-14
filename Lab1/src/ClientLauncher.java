import impl.ClientImpl;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ClientLauncher {

    private static final String CMD_OPEN = "open";
    private static final String CMD_READ = "read";
    private static final String CMD_APPEND = "append";
    private static final String CMD_CLOSE = "close";
    private static final String CMD_EXIT = "exit";

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
                    String access = tokens[2];
                    int mode = 0;  // 默认为 0

                    // 设置访问权限
                    if (access.equals("w")) {
                        mode |= 0b10;
                    } else if (access.equals("r")) {
                        mode |= 0b01;
                    } else if (access.equals("wr") || access.equals("rw")) {
                        mode |= 0b11;
                    } else {
                        // 无效的访问权限，可以根据需求进行处理
                        System.out.println("Invalid access mode");
                        printUsage();
                    }

                    int fd = client.open(filename, mode);
                    if (fd == -1) {
                        System.out.println("INFO: cannot write the same file simultaneously");
                    } else {
                        System.out.println("INFO: fd=" + fd);
                    }
                } else {
                    printUsage();
                }
                break;

            case CMD_READ:
                if (tokens.length >= 2) {
                    int fd = Integer.parseInt(tokens[1]);

                    // 从客户端读取数据
                    byte[] data = client.read(fd);

                    // 检查返回值并输出相应信息
                    if (data == null) {
                        // 没有读权限
                        System.out.println("INFO: READ not allowed");
                    } else if (data.length == 0) {
                        // 文件为空
                        System.out.println("INFO: file is empty");
                    } else {
                        // 输出文件内容
                        String content = new String(data, StandardCharsets.UTF_8);
                        System.out.println(content);
                    }

                } else {
                    printUsage();
                }
                break;

            case CMD_APPEND:
                if (tokens.length >= 3) {
                    int fd = Integer.parseInt(tokens[1]);
                    String content = input.substring(input.indexOf(tokens[2]));
                    int result = client.append(fd, content.getBytes(StandardCharsets.UTF_8));
                    if (result == -1) {
                        System.out.println("INFO: WRITE not allowed");
                    } else {
                        System.out.println("INFO: write done");
                    }
                } else {
                    printUsage();
                }
                break;

            case CMD_CLOSE:
                if (tokens.length >= 2) {
                    int fd = Integer.parseInt(tokens[1]);
                    client.close(fd);
                    System.out.println("INFO: fd " + fd + " closed");
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
        ClientImpl clientImpl = new ClientImpl();
        ClientLauncher clientLauncher = new ClientLauncher(clientImpl);
        clientLauncher.start();
    }
}