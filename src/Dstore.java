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

        // Join controller
        Socket controllerSocket = new Socket("localhost", cport);
        controllerSocket.setSoTimeout(timeout);
        PrintWriter controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        BufferedReader controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        controllerOut.println("JOIN " + port);
        System.out.println("Joined controller on port " + cport);

        // Start server for clients
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Dstore listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> handleClient(clientSocket, controllerOut, fileFolder, timeout)).start();
        }
    }

    private static void handleClient(Socket socket, PrintWriter controllerOut, String fileFolder, int timeout) {
        try (
                socket;
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ) {
            socket.setSoTimeout(timeout);
            String line = in.readLine();
            if (line == null) return;

            System.out.println("Dstore received: " + line);

            if (line.startsWith("STORE")) {
                String[] parts = line.split(" ");
                if (parts.length != 3) {
                    System.out.println("Malformed STORE command");
                    return;
                }

                String filename = parts[1];
                int filesize = Integer.parseInt(parts[2]);
                out.println("ACK");

                byte[] fileData = socket.getInputStream().readNBytes(filesize);

                synchronized (Dstore.class) {
                    FileOutputStream fos = new FileOutputStream(new File(fileFolder, filename));
                    fos.write(fileData);
                    fos.close();
                }

                System.out.println("File " + filename + " stored");
                controllerOut.println("STORE_ACK " + filename);

            } else if (line.startsWith("LOAD_DATA")) {
                String[] parts = line.split(" ");
                if (parts.length != 2) {
                    System.out.println("Malformed LOAD_DATA command");
                    return;
                }

                String filename = parts[1];
                File file = new File(fileFolder, filename);
                if (!file.exists()) {
                    System.out.println("File not found: " + filename);
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    return;
                }

                byte[] fileBytes;
                synchronized (Dstore.class) {
                    FileInputStream fis = new FileInputStream(file);
                    fileBytes = fis.readAllBytes();
                    fis.close();
                }

                OutputStream rawOut = socket.getOutputStream();
                rawOut.write(fileBytes);
                rawOut.flush();
                System.out.println("Sent file " + filename + " (" + fileBytes.length + " bytes)");

            } else if (line.startsWith("REMOVE")) {
                String[] parts = line.split(" ");
                if (parts.length != 2) {
                    System.out.println("Malformed REMOVE command");
                    return;
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
                }
            } else {
                System.out.println("Unknown command: " + line);
            }

        } catch (SocketTimeoutException ste) {
            System.out.println("Timeout occurred while communicating with client.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
