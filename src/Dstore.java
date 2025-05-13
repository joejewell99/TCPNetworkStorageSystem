import java.net.*;
import java.io.*;

public class Dstore {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        // Clear folder on startup
        File folder = new File(fileFolder);
        if (!folder.exists()) folder.mkdirs();
        for (File file : folder.listFiles()) {
            file.delete();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered. Cleaning up...");
            File folderToDelete = new File(fileFolder);
            if (folderToDelete.exists()) {
                for (File file : folderToDelete.listFiles()) {
                    file.delete();
                }
                folderToDelete.delete();
            }
        }));

        // Join controller
        Socket controllerSocket = new Socket("localhost", cport);
        PrintWriter controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        BufferedReader controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

        controllerOut.println("JOIN " + port);
        System.out.println("Joined controller on port " + cport);

        // Start controller listener thread
        new Thread(() -> handleControllerMessages(controllerIn, controllerOut, fileFolder)).start();

        // Start server for clients
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Dstore listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> handleClient(clientSocket, fileFolder, timeout, controllerOut)).start();
        }
    }

    private static void handleControllerMessages(BufferedReader controllerIn, PrintWriter controllerOut, String fileFolder) {
        try {
            String controllerLine;
            while ((controllerLine = controllerIn.readLine()) != null) {
                System.out.println("Dstore received from controller: " + controllerLine);

                if (controllerLine.startsWith("REMOVE")) {
                    String[] parts = controllerLine.split(" ");
                    if (parts.length != 2) {
                        System.out.println("Malformed REMOVE command");
                        continue;
                    }

                    String filename = parts[1];
                    File file = new File(fileFolder, filename);

                    synchronized (Dstore.class) {
                        if (file.exists()) {
                            if (file.delete()) {
                                System.out.println("Deleted file: " + filename);
                                controllerOut.println("REMOVE_ACK " + filename);
                            } else {
                                System.out.println("Failed to delete file: " + filename);
                            }
                        } else {
                            System.out.println("File does not exist for REMOVE: " + filename);
                            controllerOut.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
                        }
                        controllerOut.flush();
                    }
                } else {
                    System.out.println("Unknown command from controller: " + controllerLine);
                }
            }
        } catch (IOException e) {
            System.out.println("Controller connection lost.");
        }
    }

    private static void handleClient(Socket clientSocket, String fileFolder, int timeout, PrintWriter controllerOut) {
        try (
                clientSocket;
                BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String clientLine;
            while ((clientLine = clientIn.readLine()) != null) {
                System.out.println("Dstore received: " + clientLine);

                if (clientLine.startsWith("STORE")) {
                    String[] parts = clientLine.split(" ");
                    if (parts.length != 3) {
                        System.out.println("Malformed STORE command");
                        return;
                    }

                    String filename = parts[1];
                    int filesize = Integer.parseInt(parts[2]);
                    clientOut.println("ACK");

                    byte[] fileData = clientSocket.getInputStream().readNBytes(filesize);

                    synchronized (Dstore.class) {
                        FileOutputStream fos = new FileOutputStream(new File(fileFolder, filename));
                        fos.write(fileData);
                        fos.close();
                    }

                    System.out.println("File " + filename + " stored");
                    controllerOut.println("STORE_ACK " + filename);
                    controllerOut.flush();

                } else if (clientLine.startsWith("LOAD_DATA")) {
                    String[] parts = clientLine.split(" ");
                    if (parts.length != 2) {
                        System.out.println("Malformed LOAD_DATA command");
                        return;
                    }

                    String filename = parts[1];
                    File file = new File(fileFolder, filename);
                    if (!file.exists()) {
                        System.out.println("File not found: " + filename);
                        clientOut.println("ERROR_FILE_DOES_NOT_EXIST");
                        return;
                    }

                    byte[] fileBytes;
                    synchronized (Dstore.class) {
                        FileInputStream fis = new FileInputStream(file);
                        fileBytes = fis.readAllBytes();
                        fis.close();
                    }

                    OutputStream rawOut = clientSocket.getOutputStream();
                    rawOut.write(fileBytes);
                    rawOut.flush();
                    System.out.println("Sent file " + filename + " (" + fileBytes.length + " bytes)");

                } else {
                    System.out.println("Unknown command: " + clientLine);
                }
            }
        } catch (IOException e) {
            System.out.println("Client communication error.");
        }
    }
}
