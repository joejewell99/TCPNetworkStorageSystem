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
    private static int currentPort;
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
                    handleJoin(line, out);
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
                }
            }
        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }

    private static void handleJoin(String line, PrintWriter out) {
        try {
            int dstorePort = Integer.parseInt(line.split(" ")[1]);
            synchronized (dstorePorts) {
                if (!dstorePorts.contains(dstorePort)) {
                    dstorePorts.add(dstorePort);
                    dStores.put(dstorePort, new DstoreInfo());
                    System.out.println("Dstore joined on port: " + dstorePort);
                    out.println("JOIN_ACK");
                } else {
                    out.println("ERROR_DSTORE_ALREADY_JOINED");
                }
            }
        } catch (NumberFormatException e) {
            out.println("ERROR_INVALID_PORT");
        }
    }

    private static void handleStoreRequest(String line, PrintWriter out, Socket socket, int rep) {
        String[] parts = line.split(" ");
        String filename = parts[1];
        int fileSize = Integer.parseInt(parts[2]);

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

    private static void handleRemoveRequest(String line, PrintWriter out, Socket socket, int rep, int timeOut) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        synchronized (index) {
            FileInfo fileInfo = index.get(filename);
            if (fileInfo == null || fileInfo.getStatus() != FileStatus.STORE_COMPLETE) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }
            fileInfo.setStatus(FileStatus.REMOVE_IN_PROGRESS);
        }

        removeAckCounter.put(filename, 0);
        clientRemoveSockets.put(filename, socket);

        List<Integer> dStoresToRemove = index.get(filename).getDstores();
        for (int port : dStoresToRemove) {
            executorService.submit(() -> {
                try (Socket dstoreSocket = new Socket("localhost", port);
                     PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true)) {
                    dstoreOut.println("REMOVE " + filename);
                } catch (IOException e) {
                    System.out.println("Failed to connect to Dstore on port: " + port);
                }
            });
        }

        // Timeout thread for REMOVE operation
        executorService.submit(() -> {
            try {
                Thread.sleep(timeOut);
                if (index.get(filename).getStatus() == FileStatus.REMOVE_IN_PROGRESS) {
                    System.out.println("REMOVE timed out for " + filename);
                }
            } catch (InterruptedException ignored) {
            }
        });
    }

    private static void handleRemoveAck(String line, int rep) {
        String[] parts = line.split(" ");
        String filename = parts[1];

        int count = removeAckCounter.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);

        if (count >= rep) {
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

        public int getFileCount() {
            return fileCount;
        }

        public void incrementFileCount() {
            this.fileCount++;
        }

        public void decrementFileCount() {
            this.fileCount--;
        }
    }
}
