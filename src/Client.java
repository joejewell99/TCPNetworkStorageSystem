
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class Client {
    private final int a;
    private final int b;
    private Socket c;
    private BufferedReader d;
    private PrintWriter e;
    private int f;
    private boolean g;

    public Client(int cport, int timeout, Logger$LoggingType loggintType) {
        this.a = cport;
        this.b = timeout;
        ClientLogger.init(loggintType);
    }

    public void connect() throws IOException {
        try {
            this.disconnect();
        } catch (IOException var3) {
        }

        try {
            this.c = new Socket(InetAddress.getLoopbackAddress(), this.a);
            this.c.setSoTimeout(this.b);
            ClientLogger.getInstance().connectionEstablished(this.c.getPort());
            this.e = new PrintWriter(this.c.getOutputStream(), true);
            this.d = new BufferedReader(new InputStreamReader(this.c.getInputStream()));
            this.g = true;
        } catch (Exception var2) {
            ClientLogger.getInstance().errorConnecting(this.a);
            this.g = false;
            throw var2;
        }
    }

    public void disconnect() throws IOException {
        if (this.c != null) {
            this.c.close();
        }

        this.g = false;
    }

    public void send(String message) {
        this.e.println(message);
        ClientLogger.getInstance().messageSent(this.c.getPort(), message);
    }

    public String[] list() throws IOException, NotEnoughDstoresException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            this.e.println("LIST");
            ClientLogger.getInstance().messageSent(this.c.getPort(), "LIST");
            ClientLogger.getInstance().listStarted();

            String var1;
            try {
                var1 = this.d.readLine();
            } catch (SocketTimeoutException var4) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                ClientLogger.getInstance().listFailed();
                throw var4;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), var1);
            String[] var5;
            if (var1 != null && (var5 = var1.split(" ")).length > 0) {
                if (var5[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                    ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                    ClientLogger.getInstance().listFailed();
                    throw new NotEnoughDstoresException();
                }

                if (var5[0].equals("LIST")) {
                    String[] var2 = new String[var5.length - 1];

                    for(int var3 = 0; var3 < var2.length; ++var3) {
                        var2[var3] = var5[var3 + 1];
                    }

                    ClientLogger.getInstance().listCompleted();
                    return var2;
                }
            }

            var1 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(var1);
            ClientLogger.getInstance().listFailed();
            throw new IOException(var1);
        }
    }

    public void store(File file) throws IOException, NotEnoughDstoresException, FileAlreadyExistsException {
        String var2;
        if (!file.exists()) {
            var2 = "File to store does not exist (absolute path: " + file.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(var2);
            throw new IOException(var2);
        } else if ((var2 = file.getName()).contains(" ")) {
            String var6 = "Filename includes spaces (absolute path: " + file.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(var6);
            throw new IOException(var6);
        } else {
            byte[] var3;
            try {
                FileInputStream var4;
                var3 = (var4 = new FileInputStream(file)).readAllBytes();
                var4.close();
            } catch (IOException var5) {
                ClientLogger.getInstance().error("Error reading data from file (absolute path: " + file.getAbsolutePath() + ")");
                throw var5;
            }

            this.store(var2, var3);
        }
    }

    public void store(String filename, byte[] data) throws IOException, NotEnoughDstoresException, FileAlreadyExistsException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            String var3 = "STORE " + filename + " " + data.length;
            this.e.println(var3);
            ClientLogger.getInstance().messageSent(this.c.getPort(), var3);
            ClientLogger.getInstance().storeStarted(filename);

            String var4;
            try {
                var4 = this.d.readLine();
            } catch (SocketTimeoutException var21) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                throw var21;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), var4);
            int[] var5 = a(filename, var4);
            ClientLogger.getInstance().dstoresWhereToStoreTo(filename, var5);
            int[] var8 = var5;
            int var7 = var5.length;

            for(int var6 = 0; var6 < var7; ++var6) {
                int var24 = var8[var6];
                Socket var9 = null;
                boolean var18 = false;

                label150: {
                    try {
                        var18 = true;
                        (var9 = new Socket(InetAddress.getLoopbackAddress(), var24)).setSoTimeout(this.b);
                        ClientLogger.getInstance().connectionEstablished(var9.getPort());
                        OutputStream var10 = var9.getOutputStream();
                        PrintWriter var11 = new PrintWriter(var10, true);
                        BufferedReader var12 = new BufferedReader(new InputStreamReader(var9.getInputStream()));
                        var11.println(var3);
                        ClientLogger.getInstance().messageSent(var9.getPort(), var3);
                        ClientLogger.getInstance().storeToDstoreStarted(filename, var24);

                        String var28;
                        try {
                            var28 = var12.readLine();
                        } catch (SocketTimeoutException var20) {
                            ClientLogger.getInstance().timeoutExpiredWhileReading(var9.getPort());
                            throw var20;
                        }

                        ClientLogger.getInstance().messageReceived(var9.getPort(), var28);
                        String var27;
                        if (var28 == null) {
                            var27 = "Connection closed by Dstore ".concat(String.valueOf(var24));
                            ClientLogger.getInstance().error(var27);
                            throw new IOException(var27);
                        }

                        if (!var28.trim().equals("ACK")) {
                            var27 = "Unexpected message received from Dstore (ACK was expected): ".concat(String.valueOf(var28));
                            ClientLogger.getInstance().error(var27);
                            throw new IOException(var27);
                        }

                        ClientLogger.getInstance().ackFromDstore(filename, var24);
                        var10.write(data);
                        ClientLogger.getInstance().storeToDstoreCompleted(filename, var24);
                        var18 = false;
                        break label150;
                    } catch (Exception var22) {
                        ClientLogger.getInstance().storeToDstoreFailed(filename, var24);
                        var18 = false;
                    } finally {
                        if (var18) {
                            if (var9 != null) {
                                var9.close();
                            }

                        }
                    }

                    if (var9 == null) {
                        continue;
                    }
                }

                var9.close();
            }

            String var25;
            try {
                var25 = this.d.readLine();
            } catch (SocketTimeoutException var19) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                throw var19;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), var25);
            String var26;
            if (var25 == null) {
                var26 = "Connection closed by the Controller";
                ClientLogger.getInstance().error(var26);
                throw new IOException(var26);
            } else if (var25.trim().equals("STORE_COMPLETE")) {
                ClientLogger.getInstance().storeCompleted(filename);
            } else {
                var26 = "Unexpected message received (STORE_COMPLETE was expected): ".concat(String.valueOf(var4));
                ClientLogger.getInstance().error(var26);
                throw new IOException(var26);
            }
        }
    }

    public void wrongStore(String filename, byte[] data) throws IOException, NotEnoughDstoresException, FileAlreadyExistsException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            String data1 = "STORE " + filename + " " + (data == null ? 0 : data.length);
            this.e.println(data1);
            ClientLogger.getInstance().messageSent(this.c.getPort(), data1);
            ClientLogger.getInstance().storeStarted(filename);

            try {
                data1 = this.d.readLine();
            } catch (SocketTimeoutException var5) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                throw var5;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), data1);
            int[] var3 = a(filename, data1);
            ClientLogger.getInstance().dstoresWhereToStoreTo(filename, var3);

            String var7;
            try {
                var7 = this.d.readLine();
            } catch (SocketTimeoutException var4) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                throw var4;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), var7);
            if (var7 == null) {
                filename = "Connection closed by the Controller";
                ClientLogger.getInstance().error(filename);
                throw new IOException(filename);
            } else if (var7.trim().equals("STORE_COMPLETE")) {
                ClientLogger.getInstance().storeCompleted(filename);
            } else {
                filename = "Unexpected message received (STORE_COMPLETE was expected): ".concat(String.valueOf(data1));
                ClientLogger.getInstance().error(filename);
                throw new IOException(filename);
            }
        }
    }

    private static int[] a(String var0, String var1) throws IOException {
        if (var1 == null) {
            var0 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(var0);
            throw new IOException(var0);
        } else {
            String[] var2;
            if (!(var2 = var1.split(" "))[0].equals("STORE_TO")) {
                if (var2[0].equals("ERROR_FILE_ALREADY_EXISTS")) {
                    ClientLogger.getInstance().fileToStoreAlreadyExists(var0);
                    throw new FileAlreadyExistsException(var0);
                } else if (var2[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                    ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                    throw new NotEnoughDstoresException();
                } else {
                    var1 = "Unexpected message received: ".concat(String.valueOf(var1));
                    ClientLogger.getInstance().error(var1);
                    throw new IOException(var1);
                }
            } else {
                int[] var3 = new int[var2.length - 1];

                for(int var4 = 0; var4 < var3.length; ++var4) {
                    var3[var4] = Integer.parseInt(var2[var4 + 1]);
                }

                return var3;
            }
        }
    }

    public void load(String filename, File fileFolder) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        String var6;
        if (!fileFolder.exists()) {
            var6 = "The folder where to store the file does not exist (absolute path: " + fileFolder.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(var6);
            throw new IOException(var6);
        } else if (!fileFolder.isDirectory()) {
            var6 = "The provided folder where to store the file is not actually a directory (absolute path: " + fileFolder.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(var6);
            throw new IOException(var6);
        } else {
            byte[] var3 = this.load(filename);
            File filename1 = new File(fileFolder, filename);
            FileOutputStream filename2;
            (filename2 = new FileOutputStream(filename1)).write(var3);
            filename2.close();
        }
    }

    public byte[] load(String filename) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            this.f = 0;
            String var2;
            if (filename.contains(" ")) {
                var2 = "Filename includes spaces (filename: " + filename + ")";
                ClientLogger.getInstance().error(var2);
                throw new IOException(var2);
            } else {
                var2 = "LOAD ".concat(String.valueOf(filename));
                this.e.println(var2);
                ClientLogger.getInstance().messageSent(this.c.getPort(), var2);
                ClientLogger.getInstance().loadStarted(filename);
                byte[] var6 = null;

                try {
                    var6 = this.a(filename);
                } catch (a var5) {
                }

                while(var6 == null) {
                    String var3 = "RELOAD ".concat(String.valueOf(filename));
                    this.e.println(var3);
                    ClientLogger.getInstance().messageSent(this.c.getPort(), var3);
                    ClientLogger.getInstance().retryLoad(filename);

                    try {
                        var6 = this.a(filename);
                    } catch (a var4) {
                    }

                    if (this.f >= 10) {
                        filename = "10 Dstores contacted without succeeding, LOAD operation failed";
                        ClientLogger.getInstance().error(filename);
                        throw new IOException(filename);
                    }
                }

                return var6;
            }
        }
    }

    public byte[] wrongLoad(String filename, int howManyDstoresToContactAtMost) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            this.f = 0;
            String var3;
            if (filename.contains(" ")) {
                var3 = "Filename includes spaces (filename: " + filename + ")";
                ClientLogger.getInstance().error(var3);
                throw new IOException(var3);
            } else {
                var3 = "LOAD ".concat(String.valueOf(filename));
                this.e.println(var3);
                ClientLogger.getInstance().messageSent(this.c.getPort(), var3);
                ClientLogger.getInstance().loadStarted(filename);
                byte[] var7 = null;

                try {
                    var7 = this.b(filename);
                } catch (a var6) {
                }

                while(var7 == null) {
                    if (this.f > howManyDstoresToContactAtMost) {
                        throw new IOException("Too many Dstores contacted");
                    }

                    String var4 = "RELOAD ".concat(String.valueOf(filename));
                    this.e.println(var4);
                    ClientLogger.getInstance().messageSent(this.c.getPort(), var4);
                    ClientLogger.getInstance().retryLoad(filename);

                    try {
                        var7 = this.b(filename);
                    } catch (a var5) {
                    }
                }

                return var7;
            }
        }
    }

    private byte[] a(String var1) throws IOException {
        String var2;
        try {
            var2 = this.d.readLine();
        } catch (SocketTimeoutException var16) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            throw var16;
        }

        ClientLogger.getInstance().messageReceived(this.c.getPort(), var2);
        if (var2 == null) {
            String var21 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(var21);
            throw new IOException(var21);
        } else {
            String[] var3;
            if ((var3 = var2.split(" "))[0].equals("ERROR_LOAD")) {
                ClientLogger.getInstance().loadFailed(var1, this.f);
                throw new IOException("Load operation for file " + var1 + " failed after having contacted " + this.f + " different Dstores");
            } else if (var3[0].equals("ERROR_FILE_DOES_NOT_EXIST")) {
                ClientLogger.getInstance().fileToLoadDoesNotExist(var1);
                throw new FileDoesNotExistException(var1);
            } else if (var3[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                throw new NotEnoughDstoresException();
            } else if (!var3[0].equals("LOAD_FROM")) {
                String var22 = "Unexpected message received (expected message: LOAD_FROM): ".concat(String.valueOf(var2));
                ClientLogger.getInstance().error(var22);
                throw new IOException(var22);
            } else {
                int var4;
                int var20;
                try {
                    var4 = Integer.parseInt(var3[1]);
                    var20 = Integer.parseInt(var3[2]);
                } catch (Exception var15) {
                    String var5 = "Error parsing LOAD_FROM message to extract Dstore port and filesize. Received message: ".concat(String.valueOf(var2));
                    ClientLogger.getInstance().error(var5);
                    throw new IOException(var5);
                }

                ClientLogger.getInstance().dstoreWhereToLoadFrom(var1, var4, var20);
                Socket var19 = null;
                boolean var13 = false;

                byte[] var24;
                try {
                    var13 = true;
                    ++this.f;
                    (var19 = new Socket(InetAddress.getLoopbackAddress(), var4)).setSoTimeout(this.b);
                    ClientLogger.getInstance().connectionEstablished(var19.getPort());
                    PrintWriter var23 = new PrintWriter(var19.getOutputStream(), true);
                    InputStream var6 = var19.getInputStream();
                    String var7 = "LOAD_DATA ".concat(String.valueOf(var1));
                    var23.println(var7);
                    ClientLogger.getInstance().messageSent(var19.getPort(), var7);
                    ClientLogger.getInstance().loadFromDstore(var1, var4);

                    try {
                        var24 = var6.readNBytes(var20);
                    } catch (SocketTimeoutException var14) {
                        ClientLogger.getInstance().timeoutExpiredWhileReading(var19.getPort());
                        throw var14;
                    }

                    if (var24.length < var20) {
                        int var25 = var24.length;
                        throw new IOException("Expected to read " + var20 + " bytes, read " + var25 + " bytes instead");
                    }

                    ClientLogger.getInstance().loadCompleted(var1, var4);
                    var13 = false;
                } catch (IOException var17) {
                    ClientLogger.getInstance().loadFromDstoreFailed(var1, var4);
                    throw new a(this, var17);
                } finally {
                    if (var13) {
                        if (var19 != null) {
                            var19.close();
                        }

                    }
                }

                var19.close();
                return var24;
            }
        }
    }

    private byte[] b(String var1) throws IOException {
        String var2;
        try {
            var2 = this.d.readLine();
        } catch (SocketTimeoutException var11) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            throw var11;
        }

        ClientLogger.getInstance().messageReceived(this.c.getPort(), var2);
        if (var2 == null) {
            String var14 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(var14);
            throw new IOException(var14);
        } else {
            String[] var3;
            if ((var3 = var2.split(" "))[0].equals("ERROR_LOAD")) {
                ClientLogger.getInstance().loadFailed(var1, this.f);
                throw new IOException("Load operation for file " + var1 + " failed after having contacted " + this.f + " different Dstores");
            } else if (var3[0].equals("ERROR_FILE_DOES_NOT_EXIST")) {
                ClientLogger.getInstance().fileToLoadDoesNotExist(var1);
                throw new FileDoesNotExistException(var1);
            } else if (var3[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                throw new NotEnoughDstoresException();
            } else if (!var3[0].equals("LOAD_FROM")) {
                String var15 = "Unexpected message received (unxpected message: LOAD_FROM): ".concat(String.valueOf(var2));
                ClientLogger.getInstance().error(var15);
                throw new IOException(var15);
            } else {
                int var4;
                int var13;
                try {
                    var4 = Integer.parseInt(var3[1]);
                    var13 = Integer.parseInt(var3[2]);
                } catch (Exception var10) {
                    var2 = "Error parsing LOAD_FROM message to extract Dstore port and filesize. Received message: ".concat(String.valueOf(var2));
                    ClientLogger.getInstance().error(var2);
                    throw new IOException(var2);
                }

                ClientLogger.getInstance().dstoreWhereToLoadFrom(var1, var4, var13);
                var2 = null;

                try {
                    ++this.f;
                    throw new IOException("Just for marking purposes");
                } catch (IOException var9) {
                    ClientLogger.getInstance().loadFromDstoreFailed(var1, var4);
                    throw new a(this, var9);
                } finally {
                    if (var2 != null) {
                        var2.close();
                    }

                }
            }
        }
    }

    public void remove(String filename) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        if (!this.g) {
            throw new IOException("Client not connected");
        } else {
            String var2 = "REMOVE ".concat(String.valueOf(filename));
            this.e.println(var2);
            ClientLogger.getInstance().messageSent(this.c.getPort(), var2);
            ClientLogger.getInstance().removeStarted(filename);

            try {
                var2 = this.d.readLine();
            } catch (SocketTimeoutException var3) {
                ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                ClientLogger.getInstance().removeFailed(filename);
                throw var3;
            }

            ClientLogger.getInstance().messageReceived(this.c.getPort(), var2);
            if (var2 == null) {
                var2 = "Connection closed by the Controller";
                ClientLogger.getInstance().error(var2);
                ClientLogger.getInstance().removeFailed(filename);
                throw new IOException(var2);
            } else {
                String[] var4;
                if ((var4 = var2.split(" "))[0].equals("ERROR_FILE_DOES_NOT_EXIST")) {
                    ClientLogger.getInstance().fileToRemoveDoesNotExist(filename);
                    throw new FileDoesNotExistException(filename);
                } else if (var4[0].equals("REMOVE_COMPLETE")) {
                    ClientLogger.getInstance().removeComplete(filename);
                } else if (var4[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                    ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                    ClientLogger.getInstance().removeFailed(filename);
                    throw new NotEnoughDstoresException();
                } else {
                    var2 = "Unexpected message received. Expected message: REMOVE_COMPLETE";
                    ClientLogger.getInstance().error(var2);
                    ClientLogger.getInstance().removeFailed(filename);
                    throw new IOException(var2);
                }
            }
        }
    }

    private class a extends IOException {
        private static final long serialVersionUID = -5505350949933067170L;

        private a(Client var1, IOException var2) {
            super(var2);
        }
    }
}
