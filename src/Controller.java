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
    private static final Map<String, FileState> fileStates = new ConcurrentHashMap<>();
    private static final Map<String, Integer> removeAckCounter = new ConcurrentHashMap<>();
    private static final Map<String, Socket> clientRemoveSockets = new ConcurrentHashMap<>();

    enum FileState {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int rep = Integer.parseInt(args[1]);
        int timeOut = Integer.parseInt(args[2]);
        int reFactor = Integer.parseInt(args[3]);

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Controller running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> handleConnection(socket, rep, timeOut)).start();
        }
    }

    private static void handleConnection(Socket socket, int rep, int timeOut) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;

            while ((line = in.readLine()) != null) {
                System.out.println("Received: " + line);

                if (line.startsWith("JOIN")) {
                    int dstorePort = Integer.parseInt(line.split(" ")[1]);
                    synchronized (dstorePorts) {
                        if (!dstorePorts.contains(dstorePort)) {
                            dstorePorts.add(dstorePort);
                            System.out.println("Dstore joined on port: " + dstorePort);
                        }
                    }

                } else if (line.startsWith("STORE ")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];
                    int fileSize = Integer.parseInt(parts[2]);

                    synchronized (dstorePorts) {
                        if (dstorePorts.size() < rep) {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            continue;
                        }
                    }

                    synchronized (fileStates) {
                        FileState state = fileStates.get(filename);
                        if (state == FileState.STORE_COMPLETE || state == FileState.STORE_IN_PROGRESS || state == FileState.REMOVE_IN_PROGRESS) {
                            out.println("ERROR_FILE_ALREADY_EXISTS");
                            continue;
                        }
                        fileStates.put(filename, FileState.STORE_IN_PROGRESS);
                    }

                    fileToFileSize.put(filename, fileSize);

                    List<Integer> selectedPorts;
                    synchronized (dstorePorts) {
                        selectedPorts = new ArrayList<>(dstorePorts.subList(0, rep));
                    }

                    fileToDstores.put(filename, new HashSet<>(selectedPorts));
                    ackCounter.put(filename, 0);
                    clientStoreSockets.put(filename, socket);

                    StringBuilder response = new StringBuilder("STORE_TO");
                    for (int port : selectedPorts) {
                        response.append(" ").append(port);
                    }
                    out.println(response);

                } else if (line.startsWith("STORE_ACK")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    int count = ackCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);
                    System.out.println("ACK received for file: " + filename + " (" + count + "/" + rep + ")");

                    if (count >= rep) {
                        Socket clientSocket = clientStoreSockets.remove(filename);
                        if (clientSocket != null && !clientSocket.isClosed()) {
                            try {
                                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                                clientOut.println("STORE_COMPLETE");
                            } catch (IOException e) {
                                System.out.println("Error responding to client for " + filename);
                            }
                        }
                        ackCounter.remove(filename);
                        fileStates.put(filename, FileState.STORE_COMPLETE);
                    }

                } else if (line.startsWith("LIST")) {
                    StringBuilder listResponse = new StringBuilder("LIST");
                    synchronized (fileStates) {
                        for (Map.Entry<String, FileState> entry : fileStates.entrySet()) {
                            if (entry.getValue() == FileState.STORE_COMPLETE) {
                                listResponse.append(" ").append(entry.getKey());
                            }
                        }
                    }
                    out.println(listResponse.toString());

                } else if (line.startsWith("LOAD")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    FileState state = fileStates.get(filename);
                    if (state != FileState.STORE_COMPLETE) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        continue;
                    }

                    Set<Integer> dStoresWithFile = fileToDstores.get(filename);
                    if (dStoresWithFile == null || dStoresWithFile.isEmpty()) {
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        continue;
                    }

                    List<Integer> ports = new ArrayList<>(dStoresWithFile);
                    int chosenPort = ports.get(new Random().nextInt(ports.size()));
                    int size = fileToFileSize.get(filename);
                    out.println("LOAD_FROM " + chosenPort + " " + size);

                } else if (line.startsWith("REMOVE ")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    synchronized (fileStates) {
                        FileState state = fileStates.get(filename);
                        if (state != FileState.STORE_COMPLETE) {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            continue;
                        }
                        fileStates.put(filename, FileState.REMOVE_IN_PROGRESS);
                    }

                    removeAckCounter.put(filename, 0);
                    clientRemoveSockets.put(filename, socket);

                    Set<Integer> dstores = fileToDstores.get(filename);
                    for (int port : dstores) {
                        new Thread(() -> {
                            try (Socket dstoreSocket = new Socket("localhost", port);
                                 PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true)) {
                                dstoreOut.println("REMOVE " + filename);
                            } catch (IOException e) {
                                System.out.println("Failed to connect to Dstore on port: " + port);
                            }
                        }).start();
                    }

                    new Thread(() -> {
                        try {
                            Thread.sleep(timeOut);
                            if (fileStates.get(filename) == FileState.REMOVE_IN_PROGRESS) {
                                System.out.println("REMOVE timed out for " + filename);
                            }
                        } catch (InterruptedException ignored) {
                        }
                    }).start();

                } else if (line.startsWith("REMOVE_ACK") || line.startsWith("ERROR_FILE_DOES_NOT_EXIST")) {
                    String[] parts = line.split(" ");
                    String filename = parts[1];

                    int count = removeAckCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);

                    if (count >= rep) {
                        fileStates.remove(filename);
                        fileToDstores.remove(filename);
                        fileToFileSize.remove(filename);
                        removeAckCounter.remove(filename);

                        Socket clientSocket = clientRemoveSockets.remove(filename);
                        if (clientSocket != null && !clientSocket.isClosed()) {
                            try {
                                PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                                clientOut.println("REMOVE_COMPLETE");
                            } catch (IOException e) {
                                System.out.println("Error responding to client for REMOVE " + filename);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }
}
