# TCP Network Storage System

This project is a Java implementation of a replicated TCP network storage system. A central `Controller` tracks files and Dstore nodes, while multiple `Dstore` processes store replicated file data. Clients connect to the controller to store, load, list, and remove files.

Repository: https://github.com/joejewell99/TCPNetworkStorageSystem.git

## Project Structure

- `src/Controller.java` - starts the controller service and coordinates client/Dstore requests.
- `src/Dstore.java` - starts a storage node that joins the controller and stores file replicas.
- `src/ClientMain.java` - sample client test harness.
- `src/DummyClient.java` - simple interactive client for manually sending controller commands.
- `src/to_store/` - files that the sample client can upload.
- `src/downloads/` - files downloaded by the sample client.
- `src/dstore1/`, `src/dstore2/`, `src/dstore3/` - example Dstore storage folders.

## Requirements

- Java JDK installed and available on your `PATH`
- A terminal capable of running multiple processes at once

Check Java is installed:

```powershell
java -version
javac -version
```

## How to Run

Open a terminal in the project root:

```powershell
cd D:\Software_Engineering\TCPCourseWork
```

Compile the controller and Dstore classes:

```powershell
javac -cp src src\Controller.java src\Dstore.java
```

Start the controller:

```powershell
java -cp src Controller 12345 2 1000 0
```

Controller arguments:

```text
java Controller <cport> <replication_factor> <timeout_ms> <rebalance_period>
```

Example values:

- `12345` - controller port
- `2` - each file is replicated to 2 Dstores
- `1000` - timeout in milliseconds
- `0` - rebalance period argument

In separate terminals, start at least as many Dstores as the replication factor. For the example above, start three Dstores:

```powershell
java -cp src Dstore 12346 12345 1000 src\dstore1
```

```powershell
java -cp src Dstore 12347 12345 1000 src\dstore2
```

```powershell
java -cp src Dstore 12348 12345 1000 src\dstore3
```

Dstore arguments:

```text
java Dstore <dstore_port> <controller_port> <timeout_ms> <file_folder>
```

Run the sample client from the `src` directory so it can find `to_store` and `downloads`:

```powershell
cd src
java ClientMain 12345 1000
```

Client arguments:

```text
java ClientMain <controller_port> <timeout_ms>
```

## Manual Testing

You can also run the interactive dummy client:

```powershell
java -cp src DummyClient 12345
```

Example commands:

```text
LIST
STORE fileOne.txt 10
LOAD fileOne.txt
REMOVE fileOne.txt
exit
```

## Notes

- Start the controller before starting any Dstores.
- Start enough Dstores to satisfy the replication factor, otherwise client operations may return `ERROR_NOT_ENOUGH_DSTORES`.
- Dstore folders are cleared when each Dstore starts.
- The sample client reads upload files from `src/to_store` and writes loaded files to `src/downloads`.
- The repository currently includes generated `.class`, `.log`, documentation, and runtime storage files. A cleaner repository would normally ignore these generated outputs.
