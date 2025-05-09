//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.util.logging.Logger;

public class ClientLogger extends Logger {
    private static ClientLogger a = null;

    public static synchronized void init(Logger$LoggingType loggingType) {
        if (a == null) {
            a = new ClientLogger(loggingType);
        }

    }

    public static ClientLogger getInstance() {
        if (a == null) {
            throw new RuntimeException("ClientLogger has not been initialised yet");
        } else {
            return a;
        }
    }

    protected ClientLogger(Logger$LoggingType loggingType) {
        super(loggingType);
    }

    protected String getLogFileSuffix() {
        return "client";
    }

    public void errorConnecting(int cport) {
        this.log("Cannot connect to the Controller on port ".concat(String.valueOf(cport)));
    }

    public void storeStarted(String filename) {
        this.log("Store operation started for file ".concat(String.valueOf(filename)));
    }

    public void dstoresWhereToStoreTo(String filename, int[] dstorePorts) {
        StringBuffer filename1 = new StringBuffer("Controller replied to store " + filename + " in these Dstores: ");
        int[] var5 = dstorePorts;
        int var4 = dstorePorts.length;

        for(int var3 = 0; var3 < var4; ++var3) {
            int dstorePorts1 = var5[var3];
            filename1.append(dstorePorts1 + " ");
        }

        this.log(filename1.toString());
    }

    public void storeToDstoreStarted(String filename, int dstorePort) {
        this.log("Storing file " + filename + " to Dstore " + dstorePort);
    }

    public void ackFromDstore(String filename, int dstorePort) {
        this.log("ACK received from Dstore " + dstorePort + " to store file " + filename);
    }

    public void storeToDstoreCompleted(String filename, int dstorePort) {
        this.log("Store of file " + filename + " to Dstore " + dstorePort + " successfully completed");
    }

    public void storeToDstoreFailed(String filename, int dstorePort) {
        this.log("Store of file " + filename + " to Dstore " + dstorePort + " failed");
    }

    public void fileToStoreAlreadyExists(String filename) {
        this.log("File to store " + filename + " already exists in the data store");
    }

    public void storeCompleted(String filename) {
        this.log("Store operation for file " + filename + " completed");
    }

    public void loadStarted(String filename) {
        this.log("Load operation for file " + filename + " started");
    }

    public void retryLoad(String filename) {
        this.log("Retrying to load file ".concat(String.valueOf(filename)));
    }

    public void dstoreWhereToLoadFrom(String filename, int dstorePort, int filesize) {
        this.log("Controller replied to load file " + filename + " (size: " + filesize + " bytes) from Dstore " + dstorePort);
    }

    public void loadFromDstore(String filename, int dstorePort) {
        this.log("Loading file " + filename + " from Dstore " + dstorePort);
    }

    public void loadFromDstoreFailed(String filename, int dstorePort) {
        this.log("Load operation for file " + filename + " from Dstore " + dstorePort + " failed");
    }

    public void loadFailed(String filename, int dstoreCount) {
        this.log("Load operation for file " + filename + " failed after having contacted " + dstoreCount + " different Dstores");
    }

    public void fileToLoadDoesNotExist(String filename) {
        this.log("Load operation failed because file does not exist (filename: " + filename + ")");
    }

    public void loadCompleted(String filename, int dstoreCount) {
        this.log("Load operation of file " + filename + " from Dstore " + dstoreCount + " successfully completed");
    }

    public void removeStarted(String filename) {
        this.log("Remove operation for file " + filename + " started");
    }

    public void fileToRemoveDoesNotExist(String filename) {
        this.log("Remove operation failed because file does not exist (filename: " + filename + ")");
    }

    public void removeComplete(String filename) {
        this.log("Remove operation for file " + filename + " successfully completed");
    }

    public void removeFailed(String filename) {
        this.log("Remove operation for file " + filename + " not completed successfully");
    }

    public void listStarted() {
        this.log("List operation started");
    }

    public void listFailed() {
        this.log("List operation failed");
    }

    public void listCompleted() {
        this.log("List operation successfully completed");
    }
}
