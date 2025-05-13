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
    private int dstorefileFailures;

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

        while (true) {
            Socket socket = serverSocket.accept();
            executorService.submit(() -> handleConnection(socket, rep, timeOut, reFactor));
        }
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
                    handleStoreRequest(line, out, socket, rep);
                } else if (line.startsWith("STORE_ACK")) {
                    handleStoreAck(line, rep);
                } else if (line.startsWith("REMOVE ")) {
                    handleRemoveRequest(line, out, socket, rep, timeOut);
                } else if (line.startsWith("REMOVE_ACK")) {
                    handleRemoveAck(line, rep);
                } else if (line.startsWith("LIST")) {
                    handleListRequest(out);
                } else if (line.startsWith("LOAD")) {
                    handleLoadRequest(line, out);
                } else if (line.startsWith("ERROR_FILE_DOES_NOT_EXIST")) {
                    handleErrorFileDoesNotExist(line, rep);
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

    private static void handleStoreRequest(String line, PrintWriter out, Socket socket, int rep) {
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
            if (fileInfo != null && fileInfo.getStatus() != FileStatus.REMOVE_COMPLETE) {
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
            ackCounter.put(filename, 0);
            clientStoreSockets.put(filename, socket);

            StringBuilder response = new StringBuilder("STORE_TO");
            for (int port : selectedPorts) {
                response.append(" ").append(port);
            }
            out.println(response);
        }
    }


    private static void handleStoreAck(String line, int rep) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        int count = ackCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);
        System.out.println("ACK received for file: " + filename + " (" + count + "/" + rep + ")");

        if (count >= rep) {
            // Notify the client that the file is stored
            Socket clientSocket = clientStoreSockets.remove(filename);
            if (clientSocket != null && !clientSocket.isClosed()) {
                try {
                    PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                    FileInfo fileInfo = index.get(filename);
                    List<Integer> dStoresWithFile = fileInfo.getDstores();
                    System.out.println(dStoresWithFile);
                    clientOut.println("STORE_COMPLETE");
                } catch (IOException e) {
                    System.out.println("Error responding to client for " + filename);
                }
            }
            ackCounter.remove(filename);
            FileInfo fileInfo = index.get(filename);
            if (fileInfo != null) {
                fileInfo.setStatus(FileStatus.STORE_COMPLETE);
            }
        }
    }

    private static void handleRemoveRequest(String line, PrintWriter clientOut, Socket clientSocket, int rep, int timeoutMillis) {
        String[] parts = line.split(" ");
        if (parts.length != 2) {
            clientOut.println("ERROR_MALFORMED_REQUEST");
            return;
        }

        String filename = parts[1];

        FileInfo fileInfo;
        List<Integer> dstorePortsWithFile;
        synchronized (index) {
            fileInfo = index.get(filename);
            dstorePortsWithFile = fileInfo.getDstores();
            if (fileInfo == null || fileInfo.getStatus() != FileStatus.STORE_COMPLETE) {
                clientOut.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }
            fileInfo.setStatus(FileStatus.REMOVE_IN_PROGRESS);
        }

        removeAckCounter.put(filename, 0);
        clientRemoveSockets.put(filename, clientSocket);

        // Create and store the latch
        CountDownLatch latch = new CountDownLatch(rep);
        removeLatches.put(filename, latch);
        System.out.println(dstorePortsWithFile);

        for (int port : dstorePortsWithFile) {
            new Thread(() -> {
                try (Socket socket = new Socket("localhost", port);
                     PrintWriter dstoreOut = new PrintWriter(socket.getOutputStream(), true)) {
                    dstoreOut.println("REMOVE " + filename);
                } catch (IOException e) {
                    System.out.println("Failed to send REMOVE to Dstore " + port + ": " + e.getMessage());
                }
            }).start();
        }


        // Wait for the latch with a timeout
        new Thread(() -> {
            try {
                boolean completed = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);

                if (!completed) {
                    System.out.println("REMOVE timed out for file: " + filename);

                    removeAckCounter.remove(filename);
                    Socket sock = clientRemoveSockets.remove(filename);
                    if (sock != null && !sock.isClosed()) {
                        try {
                            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                            out.println("ERROR_TIMEOUT");
                        } catch (IOException e) {
                            System.out.println("Error sending timeout to client for " + filename);
                        }
                    }

                    synchronized (index) {
                        FileInfo info = index.get(filename);
                        if (info != null) {
                            info.setStatus(FileStatus.STORE_COMPLETE); // or consider partial retry logic
                        }
                    }

                }

            } catch (InterruptedException ignored) {
            } finally {
                removeLatches.remove(filename);
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
        String[] parts = line.split(" ");
        if (parts.length == 2) {
            String filename = parts[1];
            handleRemoveAck(filename, rep);
        } else {
            System.out.println("Malformed ERROR_FILE_DOES_NOT_EXIST message: " + line);
        }
    }


    private static void handleListRequest(PrintWriter out) {
        synchronized (index) {
            List<String> fileList = new ArrayList<>();
            for (Map.Entry<String, FileInfo> entry : index.entrySet()) {
                if (entry.getValue().getStatus() == FileStatus.STORE_COMPLETE) {
                    fileList.add(entry.getKey());
                }
            }

            if (fileList.isEmpty()) {
                out.println("LIST");
            } else {
                out.println("LIST " + String.join(" ", fileList));
            }
        }
    }

    private static void handleLoadRequest(String line, PrintWriter out) {
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

            int chosenPort = dStoresWithFile.get(new Random().nextInt(dStoresWithFile.size()));
            out.println("LOAD_FROM " + chosenPort + " " + fileInfo.getFileSize());
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

        // Decrement file counts, remove files from index if needed, etc.
        for (Map.Entry<String, FileInfo> entry : index.entrySet()) {
            FileInfo info = entry.getValue();
            if (info.getDstores() != null && info.getDstores().contains(port)) {
                System.out.println(info.getDstores());
                info.getDstores().remove((Integer) port);
                info.setDstores(info.getDstores());
                if(info.getDstores().isEmpty()){
                    index.remove(entry.getKey());
                    System.out.println("ITS EMPTY");
                }
                System.out.println(info.getDstores());
                info.setStatus(FileStatus.STORE_COMPLETE);
            }
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

