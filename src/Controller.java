import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {

    private static final List<Integer> dstorePorts = Collections.synchronizedList(new ArrayList<>());
    private static final Map<String, FileInfo> index = new ConcurrentHashMap<>();
    private static final Map<String, Integer> ackCounter = new ConcurrentHashMap<>();
    private static final Map<String, Socket> clientStoreSockets = new ConcurrentHashMap<>();
    private static final Map<String, Integer> removeAckCounter = new ConcurrentHashMap<>();
    private static final Map<String, Socket> clientRemoveSockets = new ConcurrentHashMap<>();
    private static final Map<Integer, DstoreInfo> dStores = new ConcurrentHashMap<>();
    private static final ExecutorService executorService = Executors.newCachedThreadPool(); // Using a thread pool for better management
    private static int currentRep;
    private static final ConcurrentHashMap<String, CountDownLatch> removeLatches = new ConcurrentHashMap<>();
    private static final Map<String, CountDownLatch> storeLatches = new ConcurrentHashMap<>();
    private static final Map<Integer, PrintWriter> dstoreWriters = new ConcurrentHashMap<>();
    // A map to track failed ports for each file
    private static final Map<String, List<Integer>> failedPortsForFile = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Object> fileLocks = new ConcurrentHashMap<>();

    enum FileStatus {
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

        currentRep = rep;

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Controller running on port " + port);

        // Accept Dstore connections
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    executorService.submit(() -> handleConnection(socket, rep, timeOut, reFactor));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void handleConnection(Socket socket, int rep, int timeOut, int reFactor) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;

            while ((line = in.readLine()) != null) {
                System.out.println("Received: " + line);

                if (line.startsWith("JOIN")) {
                    handleJoin(line, out, socket);
                } else if (line.startsWith("STORE ")) {
                    handleStoreRequest(line, out, socket, rep, timeOut);
                } else if (line.startsWith("STORE_ACK")) {
                    handleStoreAck(line, rep);
                } else if (line.startsWith("REMOVE ")) {
                    handleRemoveRequest(line, out, socket, rep, timeOut);
                } else if (line.startsWith("REMOVE_ACK")) {
                    handleRemoveAck(line, rep);
                } else if (line.startsWith("LIST")) {
                    handleListRequest(line, rep, out);
                } else if (line.startsWith("LOAD")) {
                    handleLoadRequest(line, out);
                } else if (line.startsWith("ERROR_FILE_DOES_NOT_EXIST")) {
                    handleErrorFileDoesNotExist(line, rep);
                } else if (line.startsWith("RELOAD")){
                    handleReloadRequest(line,out);
                }

            }
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }

    private static void handleJoin(String line, PrintWriter out, Socket dstoreSocket) {
        int dstorePort = Integer.parseInt(line.split(" ")[1]);

        synchronized (dstorePorts) {
            if (!dstorePorts.contains(dstorePort)) {
                dstorePorts.add(dstorePort);
                System.out.println(dstorePort + out.toString());
                System.out.println(dstoreSocket.getPort());
                dstoreWriters.put(dstorePort,out);
                System.out.println(dstoreWriters.keySet() + " " +dstoreWriters.get(dstorePort));
                DstoreInfo dstoreInfo = new DstoreInfo(dstoreSocket, out);
                dStores.put(dstorePort, dstoreInfo);
                System.out.println("Dstore joined on port: " + dstorePort);

                // Keep this thread alive to receive further messages (like heartbeats)
                listenToDstore(dstoreSocket, dstorePort);
                return; // Important! Return to avoid closing socket after JOIN
            } else {
                out.println("ERROR_DSTORE_ALREADY_JOINED");
            }
        }
    }

    private static void handleStoreRequest(String line, PrintWriter out, Socket socket, int rep, int timeoutMillis) {
        String[] parts = line.split(" ");
        String filename = parts[1];
        int fileSize = Integer.parseInt(parts[2]);

        if (parts.length != 3) {
            System.out.println("Malformed STORE request: " + line);
            return;
        }

        synchronized (dstorePorts) {
            if (dstorePorts.size() < rep) {
                out.println("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        synchronized (index) {
            FileInfo fileInfo = index.get(filename);
            if (fileInfo != null && fileInfo.getStatus() != FileStatus.REMOVE_COMPLETE || index.containsKey(filename)) {
                out.println("ERROR_FILE_ALREADY_EXISTS");
                return;
            }

            fileInfo = new FileInfo(filename, fileSize);
            index.put(filename, fileInfo);

            List<Integer> selectedPorts = getLeastLoadedDstores(rep);

            // Reserve the Dstores immediately so their fileCount reflects pending storage
            for (int port : selectedPorts) {
                dStores.get(port).incrementFileCount();
            }

            fileInfo.setDstores(selectedPorts);

            StringBuilder response = new StringBuilder("STORE_TO");
            for (int port : selectedPorts) {
                response.append(" ").append(port);
            }
            out.println(response);

            CountDownLatch latch = new CountDownLatch(rep);
            storeLatches.put(filename, latch);
            ackCounter.put(filename, 0);
            clientStoreSockets.put(filename, socket);

            new Thread(() -> {
                try {
                    boolean completed = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
                    if (!completed) {
                        System.out.println("STORE timed out for file: " + filename);

                        ackCounter.remove(filename);
                        storeLatches.remove(filename);

                        // Leave file in current state; spec says nothing about rollback
                        // but consider resetting to a previous safe state if desired

                    }
                } catch (InterruptedException ignored) {
                } finally {
                    storeLatches.remove(filename);
                }
            }).start();

        }
    }


    private static void handleStoreAck(String line, int rep) {
        String[] parts = line.split(" ");
        if (parts.length != 2) {
            System.out.println("Malformed STORE_ACK message: " + line);
            return;
        }

        String filename = parts[1];

        int count = ackCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);
        System.out.println("ACK received for file: " + filename + " (" + count + "/" + rep + ")");

        CountDownLatch latch = storeLatches.get(filename);
        if (latch != null) {
            latch.countDown();
        }

        if (count >= rep) {
            Socket clientSocket = clientStoreSockets.remove(filename);
            if (clientSocket != null && !clientSocket.isClosed()) {
                try {
                    PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                    FileInfo fileInfo = index.get(filename);
                    if (fileInfo != null) {
                        clientOut.println("STORE_COMPLETE");
                        fileInfo.setStatus(FileStatus.STORE_COMPLETE);
                    }
                } catch (IOException e) {
                    System.out.println("Error responding to client for " + filename);
                    index.remove(filename);
                }
            }
            ackCounter.remove(filename);
            storeLatches.remove(filename);
        }
    }




    private static synchronized void handleRemoveRequest(String line, PrintWriter clientOut, Socket clientSocket, int rep, int timeoutMillis) {
        String[] parts = line.split(" ");
        if (parts.length != 2) {
            System.out.println("ERROR_MALFORMED_REQUEST");
            return;
        }

        String filename = parts[1];

        synchronized (dstorePorts) {
            if (dstorePorts.size() < rep) {
                clientOut.println("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        FileInfo fileInfo;
        List<Integer> dstorePortsWithFile;
        synchronized (index) {
            fileInfo = index.get(filename);
            if (fileInfo == null || fileInfo.getStatus() != FileStatus.STORE_COMPLETE) {
                clientOut.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            fileInfo.setStatus(FileStatus.REMOVE_IN_PROGRESS);
            dstorePortsWithFile = fileInfo.getDstores();
        }

        // Track acknowledgements and client socket
        removeAckCounter.put(filename, 0);
        clientRemoveSockets.put(filename, clientSocket);

        // Set up latch for timeout
        CountDownLatch latch = new CountDownLatch(rep);
        removeLatches.put(filename, latch);
        System.out.println("Sending REMOVE to dstores: " + dstorePortsWithFile);

        // Send REMOVE to each Dstore
        for (int port : dstorePortsWithFile) {
            executorService.submit(() -> {
                PrintWriter dstoreOut = dstoreWriters.get(port);{
                   dstoreOut.println("REMOVE " + filename);
               }
            });
        }

        // Wait for the latch with a timeout
        new Thread(() -> {
            try {
                boolean completed = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);

                if (!completed) {
                    System.out.println("REMOVE timed out for file: " + filename);
                    // No further action per spec, but clean up temporary state
                    removeAckCounter.remove(filename);
                    removeLatches.remove(filename);
                    clientRemoveSockets.remove(filename);
                    // Leave index entry in REMOVE_IN_PROGRESS for rebalancing
                }

            } catch (InterruptedException ignored) {
                System.out.println("we got interupted");
            }
        }).start();
    }



    private static void handleRemoveAck(String line, int rep) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        if (parts.length != 2) {
            System.out.println("Malformed REMOVE_ACK message: " + line);
            return;
        }

        int count = removeAckCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);

        // Count down the latch if present
        CountDownLatch latch = removeLatches.get(filename);
        if (latch != null) {
            latch.countDown();
        }

        int dstorefileFailures = rep - index.get(filename).dstores.size();

        if (count + dstorefileFailures >= rep) {
            FileInfo fileInfo = index.get(filename);
            if (fileInfo != null) {
                List<Integer> dStoresWithFile = fileInfo.getDstores();
                for (int currentPort : dStoresWithFile) {
                    dStores.get(currentPort).decrementFileCount();
                }
                System.out.println("fileRemoved");
                index.remove(filename);
                removeAckCounter.remove(filename);
            }

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


    private static void handleErrorFileDoesNotExist(String line, int rep) {
        handleRemoveAck(line, rep);

    }


    private static void handleListRequest(String line, int rep, PrintWriter out) {
        String[] parts = line.split(" ");
        if (parts.length != 1) {
            System.out.println("Malformed LIST request: ");
            return;
        }

        synchronized (dstorePorts) {
            if (dstorePorts.size() < rep) {
                out.println("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        synchronized (index) {
            List<String> fileList = new ArrayList<>();
            for (Map.Entry<String, FileInfo> entry : index.entrySet()) {
                if (entry.getValue().getStatus() == FileStatus.STORE_COMPLETE) {
                    fileList.add(entry.getKey());
                }
            }

            if (fileList.isEmpty()) {
                out.println("LIST ");
            } else {
                out.println("LIST " + String.join(" ", fileList));
            }
        }
    }

    public static synchronized void handleLoadRequest(String line, PrintWriter out) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        synchronized (index) {
            FileInfo fileInfo = index.get(filename);
            if (fileInfo == null || fileInfo.getStatus() != FileStatus.STORE_COMPLETE) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            List<Integer> dStoresWithFile = fileInfo.getDstores();
            if (dStoresWithFile == null || dStoresWithFile.isEmpty()) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // Get the list of failed ports for this file (if any)
            List<Integer> failedPorts = failedPortsForFile.getOrDefault(filename, new ArrayList<>());

            // Filter out the failed ports from the available DStores
            List<Integer> availablePorts = new ArrayList<>(dStoresWithFile);
            availablePorts.removeAll(failedPorts);

            // If all ports have failed, return an error
            if (availablePorts.isEmpty()) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // Randomly select a port that has not been used before
            int chosenPort = availablePorts.get(new Random().nextInt(availablePorts.size()));

            // Send the load request to the chosen port
            out.println("LOAD_FROM " + chosenPort + " " + fileInfo.getFileSize());

            // If the request fails, we add the port to the list of failed ports
            failedPorts.add(chosenPort);
            failedPortsForFile.put(filename, failedPorts);
        }
    }

    public static void handleReloadRequest(String line, PrintWriter out) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        synchronized (index) {
            FileInfo fileInfo = index.get(filename);
            if (fileInfo == null || fileInfo.getStatus() != FileStatus.STORE_COMPLETE) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            List<Integer> dStoresWithFile = fileInfo.getDstores();
            if (dStoresWithFile == null || dStoresWithFile.isEmpty()) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // Get the list of failed ports for this file
            List<Integer> failedPorts = failedPortsForFile.getOrDefault(filename, new ArrayList<>());

            // Filter out the failed ports from the available DStores
            List<Integer> availablePorts = new ArrayList<>(dStoresWithFile);
            availablePorts.removeAll(failedPorts);

            // If all ports have failed, return an error
            if (availablePorts.isEmpty()) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            // Randomly select a port that has not been used before
            int chosenPort = availablePorts.get(new Random().nextInt(availablePorts.size()));

            // Send the load request to the chosen port
            out.println("LOAD_FROM " + chosenPort + " " + fileInfo.getFileSize());

            // If the request fails, we add the port to the list of failed ports
            failedPorts.add(chosenPort);
            failedPortsForFile.put(filename, failedPorts);
        }
    }

    private static synchronized List<Integer> getLeastLoadedDstores(int rep) {
        // Create list of (port, fileCount) pairs
        List<Map.Entry<Integer, Integer>> entries = new ArrayList<>();
        for (Map.Entry<Integer, DstoreInfo> entry : dStores.entrySet()) {
            entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().getFileCount()));
        }

        // Sort by file count (ascending)
        entries.sort(Comparator.comparingInt(Map.Entry::getValue));

        // Collect the ports for the lowest file counts, respecting ties
        List<Integer> result = new ArrayList<>();
        int i = 0;
        while (i < entries.size() && result.size() < rep) {
            int currentCount = entries.get(i).getValue();
            while (i < entries.size() && entries.get(i).getValue() == currentCount) {
                result.add(entries.get(i).getKey());
                i++;
            }
        }
        System.out.println(result);
        // Trim to exactly `rep` if more than needed (e.g., from ties)
        return result.subList(0, rep);
    }




    // FileInfo and DstoreInfo Classes

    static class FileInfo {
        private final String filename;
        private final int fileSize;
        private FileStatus status;
        private List<Integer> dstores;

        public FileInfo(String filename, int fileSize) {
            this.filename = filename;
            this.fileSize = fileSize;
            this.status = FileStatus.STORE_IN_PROGRESS;
        }

        public String getFilename() {
            return filename;
        }

        public int getFileSize() {
            return fileSize;
        }

        public FileStatus getStatus() {
            return status;
        }

        public void setStatus(FileStatus status) {
            this.status = status;
        }

        public List<Integer> getDstores() {
            return dstores;
        }

        public void setDstores(List<Integer> dstores) {
            this.dstores = dstores;
        }
    }

    static class DstoreInfo {
        private int fileCount;
        private Socket socket;
        private PrintWriter out;
        private long lastHeartbeat;

        public DstoreInfo(Socket socket, PrintWriter out) {
            this.socket = socket;
            this.out = out;
            this.lastHeartbeat = System.currentTimeMillis();
        }

        public int getFileCount() {
            return fileCount;
        }

        public void incrementFileCount() {
            this.fileCount++;
        }

        public void decrementFileCount() {
            this.fileCount--;
        }

        public Socket getSocket() {
            return socket;
        }

        public PrintWriter getOut() {
            return out;
        }

        public void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }

        public long getLastHeartbeat() {
            return lastHeartbeat;
        }
    }

    private static void listenToDstore(Socket socket, int dstorePort) {
        executorService.submit(() -> {
            try (
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                String msg;
                while ((msg = in.readLine()) != null) {
                    System.out.println("From Dstore " + dstorePort + ": " + msg);
                    if (msg.equals("HEARTBEAT")) {
                        dStores.get(dstorePort).updateHeartbeat();
                    } else if (msg.startsWith("STORE_ACK")) {
                        handleStoreAck(msg, currentRep);  // pass rep
                    } else if (msg.startsWith("REMOVE_ACK")) {
                        handleRemoveAck(msg, currentRep);  // pass rep
                    }
                    // Add more handlers as needed
                }
            } catch (IOException e) {
                System.out.println("Dstore " + dstorePort + " disconnected.");
                handleDstoreCrash(dstorePort);
            }
        });
    }

    private static synchronized void handleDstoreCrash(int port) {
        System.out.println("Handling crash for Dstore on port " + port);
        List<String> toRemove = new ArrayList<>();
        // Decrement file counts, remove files from index if needed, etc.
        synchronized (index) {
            for (Map.Entry<String, FileInfo> entry : index.entrySet()) {
                FileInfo info = entry.getValue();
                synchronized (info) {
                    if (info.getDstores() != null && info.getDstores().contains(port)) {
                        System.out.println(info.getDstores());
                        info.getDstores().remove((Integer) port);
                        info.setDstores(info.getDstores());
                        if (info.getDstores().isEmpty()) {
                            toRemove.add(entry.getKey());
                            System.out.println("ITS EMPTY");
                        }
                        System.out.println(info.getDstores());
                        info.setStatus(FileStatus.STORE_COMPLETE);
                    }
                }
            }
        }

        for (String key : toRemove) {
            index.remove(key);
        }



        try {
            DstoreInfo info = dStores.get(port);
            if (info != null && info.getSocket() != null) {
                info.getSocket().close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Remove Dstore from tracking
        dStores.remove(port);
        dstorePorts.remove((Integer) port);

    }


}

