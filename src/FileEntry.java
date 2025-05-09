import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileEntry {
    enum State {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS
    }

    private final String filename;
    private final int fileSize;
    private volatile State state;
    private final Set<Integer> dstorePorts;

    public FileEntry(String filename, int fileSize) {
        this.filename = filename;
        this.fileSize = fileSize;
        this.state = State.STORE_IN_PROGRESS;
        this.dstorePorts = ConcurrentHashMap.newKeySet();
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
    }

    public int getFileSize() {
        return fileSize;
    }

    public synchronized Set<Integer> getDstores() {
        return new HashSet<>(dstorePorts);
    }

    public synchronized void addDstore(int port) {
        dstorePorts.add(port);
    }

    public synchronized void clearDstores() {
        dstorePorts.clear();
    }

    public synchronized boolean hasDstore(int port) {
        return dstorePorts.contains(port);
    }
}
