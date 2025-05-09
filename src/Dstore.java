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

        // Create folder if doesn't exist
        File folder = new File(fileFolder);
        if (!folder.exists()) folder.mkdirs();

        // 1. Join controller
        Socket controllerSocket = new Socket("localhost", cport);
        PrintWriter controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        controllerOut.println("JOIN " + port);
        System.out.println("Joined controller on port " + cport);

        // 2. Start server for client
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Dstore listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> handleClient(clientSocket, controllerOut, fileFolder)).start();
        }
    }

    private static void handleClient(Socket socket, PrintWriter controllerOut, String fileFolder) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            DataInputStream dataIn = new DataInputStream(socket.getInputStream());

            String line = in.readLine();
            System.out.println("Dstore received: " + line);
            if (line.startsWith("STORE")) {
                String[] parts = line.split(" ");
                String filename = parts[1];
                int filesize = Integer.parseInt(parts[2]);

                // Acknowledge store
                out.println("ACK");

                // Read file content using readNBytes
                byte[] fileData = socket.getInputStream().readNBytes(filesize);

                // Save to Folder specified
                FileOutputStream fos = new FileOutputStream(new File(fileFolder, filename));
                fos.write(fileData);
                fos.close();

                System.out.println("File " + filename + " stored");

                // Notify controller
                controllerOut.println("STORE_ACK " + filename);
            } else if (line.startsWith("LOAD_DATA ")) {
                String[] parts = line.split(" ");
                String filename = parts[1];

                File file = new File(fileFolder, filename);
                if (!file.exists()) {
                    System.out.println("File not found: " + filename);
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                } else {
                    try {
                        // Read file into byte array
                        FileInputStream fis = new FileInputStream(file);
                        byte[] fileBytes = fis.readAllBytes();
                        fis.close();

                        // Send raw data to client
                        OutputStream rawOut = socket.getOutputStream();
                        rawOut.write(fileBytes);
                        rawOut.flush();

                        System.out.println("Sent file " + filename + " (" + fileBytes.length + " bytes)");
                    } catch (IOException e) {
                        System.out.println("Failed to send file: " + filename);
                        e.printStackTrace();
                    }
                }
            } else if (line.startsWith("REMOVE")) {
                String[] parts = line.split(" ");
                String filename = parts[1];

                File file = new File(fileFolder, filename);
                if (file.exists()) {
                    if (file.delete()) {
                        System.out.println("Deleted file: " + filename);
                        controllerOut.println("REMOVE_ACK " + filename);
                    } else {
                        System.out.println("Failed to delete file: " + filename);
                        // You might send an error if needed, but spec says just treat as stuck
                    }
                } else {
                    System.out.println("File does not exist for REMOVE: " + filename);
                    controllerOut.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
