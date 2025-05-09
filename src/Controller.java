import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {

    private static final Map<String, Set<Integer>> fileToDstores = new ConcurrentHashMap<>();
    private static final List<Integer> dstorePorts = Collections.synchronizedList(new ArrayList<>());
    private static final Map<String, Integer> ackCounter = new ConcurrentHashMap<>();
    private static final Map<String, Socket> clientStoreSockets = new ConcurrentHashMap<>();
    private static final Map<String, Integer> fileToFileSize = new ConcurrentHashMap<>();
    public static Set<String> allFilesList = new HashSet<>();
    private static final Map<String, Integer> removeAckCounter = new ConcurrentHashMap<>();
    private static final Map<String, Socket> clientRemoveSockets = new ConcurrentHashMap<>();
    private static final Set<String> removingFiles = ConcurrentHashMap.newKeySet();



    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int rep = Integer.parseInt(args[1]);
        int timeOut = Integer.parseInt(args[2]);
        int reFactor = Integer.parseInt(args[3]);
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Controller running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> handleConnection(socket, rep)).start();
        }
    }

    private static void handleConnection(Socket socket, int rep) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;

            while ((line = in.readLine()) != null) {
                System.out.println("Received: " + line);

                if (line.startsWith("JOIN")) {
                    int dstorePort = Integer.parseInt(line.split(" ")[1]);
                    if (!dstorePorts.contains(dstorePort)) {
                        dstorePorts.add(dstorePort);
                        System.out.println("Dstore joined on port: " + dstorePort);
                    }

                } else if (line.startsWith("STORE ")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];
                    int fileSize = Integer.parseInt(parts[2]);
                    allFilesList.add(filename);


                    fileToFileSize.put(filename, fileSize);
                    System.out.println("File entry updated: " + filename + " = " + fileSize);

                    System.out.println("STORE request for file: " + filename + ", size: " + fileSize);

                    // Select R Dstores
                    if (dstorePorts.size() < rep) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        return;
                    }

                    // making the controller to client response
                    List<Integer> selectedPorts = dstorePorts.subList(0, rep);
                    StringBuilder response = new StringBuilder("STORE_TO");
                    for (int port : selectedPorts) {
                        response.append(" ").append(port);
                    }

                    // Track store progress
                    fileToDstores.put(filename, new HashSet<>(selectedPorts));
                    ackCounter.put(filename, 0);
                    clientStoreSockets.put(filename, socket); // Save socket for sending STORE_COMPLETE later

                    out.println(response.toString());

                } else if (line.startsWith("STORE_ACK")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    ackCounter.put(filename, ackCounter.getOrDefault(filename, 0) + 1);
                    int currentAckCount = ackCounter.get(filename);

                    System.out.println("ACK received for file: " + filename + " (" + currentAckCount + "/" + rep + ")");

                    if (currentAckCount >= rep) {
                        // Send STORE_COMPLETE to client
                        Socket clientSocket = clientStoreSockets.get(filename);
                        if (clientSocket != null && !clientSocket.isClosed()) {
                            PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                            clientOut.println("STORE_COMPLETE");
                            System.out.println("STORE_COMPLETE sent to client for file: " + filename);
                        }
                        // Clean up
                        ackCounter.remove(filename);
                        clientStoreSockets.remove(filename);
                    }

                } else if (line.startsWith("LIST")) {
                    String fileList = String.join(" ", allFilesList);
                    out.println("LIST " + fileList);

                } else if (line.startsWith("LOAD")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    System.out.println("Loading File Name: " + filename);

                    Set<Integer> dStoresWithFile = fileToDstores.get(filename);
                    if (dStoresWithFile == null || dStoresWithFile.isEmpty()) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        return;
                    }

                    List<Integer> portNumsWhereFileIs = new ArrayList<>(dStoresWithFile);
                    int randomChosenPort = portNumsWhereFileIs.get(new Random().nextInt(portNumsWhereFileIs.size()));

                    Integer fileSize = fileToFileSize.get(filename);
                    if (fileSize == null) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        return;
                    }

                    out.println("LOAD_FROM " + randomChosenPort + " " + fileSize);
                } else if (line.startsWith("REMOVE ")) {
                    String[] parts = line.split(" ");
                    if (parts.length < 2) {
                        System.out.println("Malformed REMOVE: " + line);
                        continue;
                    }
                    String filename = parts[1];

                    if (dstorePorts.size() < rep) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        continue;
                    }

                    if (!fileToDstores.containsKey(filename)) {
                        out.println("REMOVE_COMPLETE");
                        continue;
                    }

                    System.out.println("Initiating REMOVE for " + filename);

                    removingFiles.add(filename);
                    removeAckCounter.put(filename, 0);
                    clientRemoveSockets.put(filename, socket);

                    Set<Integer> dstores = fileToDstores.get(filename);
                    for (int dstorePort : dstores) {
                        try {
                            Socket dstoreSocket = new Socket("localhost", dstorePort);
                            PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                            dstoreOut.println("REMOVE " + filename);
                        } catch (IOException e) {
                            System.out.println("Failed to connect to Dstore on port: " + dstorePort);
                            // Dstore failure; we do nothing (file stays in "removing" state)
                        }
                    }

                    // Start timeout timer
                    new Thread(() -> {
                        try {
                            Thread.sleep(3000); // or use 'timeOut' if needed
                            if (removingFiles.contains(filename)) {
                                System.out.println("REMOVE timed out for " + filename);
                                // Do nothing further, file remains in removing state
                            }
                        } catch (InterruptedException ignored) {}
                    }).start();
                }
                else if (line.startsWith("REMOVE_ACK") || line.startsWith("ERROR_FILE_DOES_NOT_EXIST")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    int count = removeAckCounter.getOrDefault(filename, 0) + 1;
                    removeAckCounter.put(filename, count);
                    System.out.println("REMOVE_ACK or file missing for " + filename + ": " + count);

                    if (count >= rep) {
                        removingFiles.remove(filename);
                        fileToDstores.remove(filename);
                        fileToFileSize.remove(filename);
                        allFilesList.remove(filename);
                        removeAckCounter.remove(filename);

                        Socket clientSocket = clientRemoveSockets.remove(filename);
                        if (clientSocket != null && !clientSocket.isClosed()) {
                            PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                            clientOut.println("REMOVE_COMPLETE");
                            System.out.println("REMOVE_COMPLETE sent for " + filename);
                        }
                    }
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
