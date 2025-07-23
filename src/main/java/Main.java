import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
  static HashMap<String, StoredValue> storedData = new HashMap<>();
  static int port = 6379;
  static String role = "master";
  static String masterServerAddress = "";
  static String masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  static int masterReplOffset = 0;
  static final byte[] emptyRDB = Base64.getDecoder().decode(
      "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
  static ArrayList<OutputStream> replicaOutputStreams = new ArrayList<>();

  public static void main(String[] args) {
    ServerSocket serverSocket = null;
    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("--")) {
          switch (args[i].toLowerCase()) {
            case "--port":
              port = Integer.parseInt(args[i + 1]);
              i++;
              break;
            case "--replicaof":
              role = "slave";
              masterServerAddress = args[i + 1];
              String[] address = masterServerAddress.split(" ");
              String hostAddr = address[0];
              int portAddr = Integer.valueOf(address[1]);
              Socket master = new Socket(hostAddr, portAddr);
              OutputStream outputStream = master.getOutputStream();
              InputStream inputStream = master.getInputStream();
              outputStream.write("*1\r\n$4\r\nPING\r\n".getBytes());
              outputStream.flush();
              readSimpleString(inputStream); // OK;
              outputStream
                  .write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n").getBytes());
              outputStream.flush();
              readSimpleString(inputStream); // OK
              outputStream.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
              outputStream.flush();
              readSimpleString(inputStream); // OK
              outputStream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
              outputStream.flush();
              readSimpleString(inputStream); // FULLRESYNC ...
              readBulkString(inputStream, false); // RDB file

              new Thread(() -> {
                try {
                  while (true) {
                    try {
                      String[] line = readLineFromInputStream(inputStream);
                      System.out.println("a " + String.join(" ", line));
                      parseCommand(inputStream, outputStream, line, new BooleanWrapper(false), null);
                    } catch (EOFException e) {
                      break;
                    }
                  }
                } catch (Exception e) {
                  System.out.println("Replication error: " + e.toString());
                }
              }).start();

              i++;
              break;
          }
        }
      }
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println(e.toString());
    }
  }

  static void handleClient(Socket clientSocket) {
    try (clientSocket; // automatically closes socket at the end
        OutputStream outputStream = clientSocket.getOutputStream();
        InputStream inputStream = clientSocket.getInputStream();) {
      System.out.println("New client connected");
      ArrayList<String[]> transaction = new ArrayList<>();
      BooleanWrapper transactionQueued = new BooleanWrapper(false);
      while (true) {
        try {
          String[] line = readLineFromInputStream(inputStream);
          parseCommand(inputStream, outputStream, line, transactionQueued, transaction);
        } catch (EOFException e) {
          break;
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }

  static String[] readLineFromInputStream(InputStream inputStream) throws EOFException, IOException {
    String[] line;
    int firstByte = inputStream.read();
    if (firstByte == -1)
      throw new EOFException("break now"); // client closed connection
    if ((char) firstByte == '*') {
      int length = 0;
      int digit = 0;
      while ((digit = inputStream.read()) != '\r') {
        length = length * 10 + (digit - '0');
      }
      inputStream.read(); // \n
      line = new String[length];
    } else {
      System.out.println("Byte: " + firstByte + ", char: " + (char) firstByte);
      throw new IOException("Doesn't start with *");
    }

    for (int i = 0; i < line.length; i++) {
      try {
        line[i] = readBulkString(inputStream, true);
      } catch (IllegalArgumentException e) {
        throw new IOException("Not bulk string, probably should deal with this (" + e.getMessage() + ")");
      }
    }
    return line;
  }

  static void storeWithExpiry(String key, StoredValue value, long expiryMillis) {
    storedData.put(key, value);
    Timer timer = new Timer();
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            storedData.remove(key);
          }
        },
        expiryMillis);
  }

  static void parseCommand(InputStream inputStream, OutputStream outputStream, String[] line,
      BooleanWrapper transactionQueued, ArrayList<String[]> transaction)
      throws InterruptedException, IOException {
    String command = line[0];
    if (transactionQueued.value) {
      if (command.equalsIgnoreCase("EXEC")) {
        transactionQueued.value = false;
        String[] commandReturns = new String[transaction.size()];
        outputStream.write(("*" + commandReturns.length + "\r\n").getBytes());
        outputStream.flush();
        for (int i = 0; i < transaction.size(); i++) {
          parseCommand(inputStream, outputStream, transaction.get(i), new BooleanWrapper(false), null);
        }
        outputStream.flush();
      } else if (command.equalsIgnoreCase("DISCARD")) {
        transaction.clear();
        transactionQueued.value = false;
        outputStream.write("+OK\r\n".getBytes());
        outputStream.flush();
        return;
      } else {
        transaction.add(line);
        outputStream.write(simpleString("QUEUED").getBytes());
        outputStream.flush();
        return;
      }
    } else {
      switch (command.toUpperCase()) {
        case "PING": {
          outputStream.write("+PONG\r\n".getBytes());
          outputStream.flush();
          break;
        }
        case "ECHO": {
          String toEcho = line[1];
          outputStream.write(("$" + toEcho.length() + "\r\n" + toEcho + "\r\n").getBytes());
          outputStream.flush();
          break;
        }
        case "SET": {
          String key = line[1];
          String value = line[2];
          if (line.length == 3) {
            storedData.put(key, new StoredString(value));
          } else {
            if (line[3].equalsIgnoreCase("PX")) {
              long expiry = Long.parseLong(line[4]);
              storeWithExpiry(key, new StoredString(value), expiry);
            }
          }
          outputStream.write("+OK\r\n".getBytes());
          outputStream.flush();

          for (OutputStream replicaOutputStream : replicaOutputStreams) {
            replicaOutputStream.write(bulkStringArray(line).getBytes());
          }
          break;
        }
        case "GET": {
          String key = line[1];
          StoredValue value = storedData.get(key);
          if (value == null) {
            outputStream.write("$-1\r\n".getBytes());
          } else if (value instanceof StoredString) {
            outputStream.write(((StoredString) value).getOutput().getBytes());
          } else {
            outputStream.write("$-1\r\n".getBytes());
          }
          outputStream.flush();
          break;
        }
        case "TYPE": {
          String key = line[1];
          StoredValue value = storedData.get(key);
          if (value == null) {
            outputStream.write("+none\r\n".getBytes());
          } else {
            outputStream.write(simpleString(value.type).getBytes());
          }
          outputStream.flush();
          break;
        }
        case "XADD": {
          String key = line[1];
          StoredStream stream;
          if (storedData.containsKey(key)) {
            stream = (StoredStream) storedData.get(key);
          } else {
            stream = new StoredStream();
            storedData.put(key, stream);
          }
          try {
            String id = line[2];
            HashMap<String, String> addedEntries = new HashMap<>();
            for (int i = 3; i < line.length; i += 2) {
              addedEntries.put(line[i], line[i + 1]);
            }
            String returnId = stream.addEntries(id, addedEntries);
            outputStream.write(bulkString(returnId).getBytes());
            outputStream.flush();
          } catch (RedisException e) {
            outputStream.write(simpleError(e.getMessage()).getBytes());
            outputStream.flush();
          }
          break;
        }
        case "XRANGE": {
          String key = line[1];
          StoredStream stream = (StoredStream) storedData.get(key);
          ArrayList<StreamEntry> range = stream.getRange(line[2], line[3]);
          if (range.size() == 0) {
            outputStream.write("$-1\r\n".getBytes());
            outputStream.flush();
            break;
          }
          String returnString = "*" + range.size() + "\r\n";
          for (StreamEntry entry : range) {
            returnString += "*2\r\n";
            returnString += bulkString(entry.id);
            returnString += "*" + (entry.values.size() * 2) + "\r\n";
            for (String entryKey : entry.values.keySet()) {
              returnString += bulkString(entryKey);
              returnString += bulkString(entry.values.get(entryKey));
            }
          }
          outputStream.write(returnString.getBytes());
          outputStream.flush();
          break;
        }
        case "XREAD": {
          int streamsIndex = 0;
          while (streamsIndex < line.length && !line[streamsIndex].equalsIgnoreCase("STREAMS")) {
            streamsIndex++;
          }
          int numStreams = (line.length - 1 - streamsIndex) / 2;
          String[] topIds = new String[numStreams];
          if (line[1].equalsIgnoreCase("BLOCK")) {
            for (int i = streamsIndex + 1; i <= streamsIndex + numStreams; i++) {
              StoredStream stream = (StoredStream) storedData.get(line[i]);
              topIds[i - streamsIndex - 1] = stream.entries.get(stream.entries.size() - 1).id;
            }
            long blockTime = Long.parseLong(line[2]);
            if (blockTime == 0) {
              // BLOCK 0: wait indefinitely for data
              boolean dataAvailable = false;
              while (!dataAvailable) {
                for (int i = streamsIndex + 1; i <= streamsIndex + numStreams; i++) {
                  String streamKey = line[i];
                  StoredStream stream = (StoredStream) storedData.get(streamKey);
                  String topId = topIds[i - streamsIndex - 1];

                  synchronized (stream) {
                    ArrayList<StreamEntry> newEntries = stream.getRangeFromStartExclusive(topId);
                    if (!newEntries.isEmpty()) {
                      dataAvailable = true;
                      break;
                    } else {
                      stream.wait();
                    }
                  }
                }
              }
            } else {
              Thread.sleep(blockTime);
            }
          }
          String returnString = "*" + numStreams + "\r\n";
          for (int i = streamsIndex + 1; i <= streamsIndex + numStreams; i++) {
            String key = line[i];
            returnString += "*2\r\n" + bulkString(key);
            StoredStream stream = (StoredStream) storedData.get(key);
            String startId = line[i + numStreams];
            ArrayList<StreamEntry> range;
            if (startId.equals("$")) {
              range = stream.getRangeFromStartExclusive(topIds[i - streamsIndex - 1]);
            } else {
              range = stream.getRangeFromStartExclusive(startId);
            }

            if (range.size() == 0) {
              outputStream.write("$-1\r\n".getBytes());
              outputStream.flush();
              break;
            }
            returnString += "*" + range.size() + "\r\n";
            for (int j = 0; j < range.size(); j++) {
              StreamEntry entry = range.get(j);
              returnString += "*2\r\n";
              returnString += bulkString(entry.id);
              returnString += "*" + (entry.values.size() * 2) + "\r\n";
              for (String entryKey : entry.values.keySet()) {
                returnString += bulkString(entryKey);
                returnString += bulkString(entry.values.get(entryKey));
              }
            }
          }
          outputStream.write(returnString.getBytes());
          outputStream.flush();
          break;
        }
        case "INCR": {
          String key = line[1];
          StoredValue storedValue = storedData.get(key);
          if (storedValue == null) {
            storedData.put(key, new StoredString("1"));
            outputStream.write(":1\r\n".getBytes());
          } else if (storedValue instanceof StoredString) {
            try {
              StoredString storedString = (StoredString) storedValue;
              int newVal = Integer.parseInt(storedString.value) + 1;
              storedString.value = String.valueOf(newVal);
              outputStream.write((":" + newVal + "\r\n").getBytes());
            } catch (NumberFormatException e) {
              outputStream.write(simpleError("ERR value is not an integer or out of range").getBytes());
            }
          } else {
            outputStream.write(simpleError("Incremented value isn't a string").getBytes());
          }
          outputStream.flush();
          break;
        }
        case "MULTI": {
          transactionQueued.value = true;
          transaction.clear();
          outputStream.write("+OK\r\n".getBytes());
          outputStream.flush();
          break;
        }
        case "EXEC": { // Won't ever be here if transactionQueued == true
          outputStream.write(simpleError("ERR EXEC without MULTI").getBytes());
          outputStream.flush();
          break;
        }
        case "DISCARD": { // Won't ever be here if transactionQueued == true
          outputStream.write(simpleError("ERR DISCARD without MULTI").getBytes());
          outputStream.flush();
          break;
        }
        case "INFO": {
          String returnString = "";
          for (int i = 1; i < line.length; i++) {
            String section = line[i].toLowerCase();
            switch (section) {
              case "replication":
                if (i > 1) {
                  returnString += "\r\n";
                }
                returnString += "# Replication\r\n";
                returnString += "role:" + role;
                returnString += "master_replid:" + masterReplid;
                returnString += "master_repl_offset:" + masterReplOffset;
                outputStream.write(bulkString(returnString).getBytes());
                outputStream.flush();
                break;
              default:
                outputStream.write(simpleError("ERR: Invalid section [" + section + "] for INFO command").getBytes());
                outputStream.flush();
                break;
            }
          }
        }
        case "REPLCONF":
          if (line.length > 1) {
            switch (line[1].toUpperCase()) {
              case "GETACK":
                if (line[2].equals("*")) {
                  int offset = 0;
                  outputStream.write(("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n" + offset + "\r\n").getBytes());
                  outputStream.flush();
                }
                break;
              default:
                outputStream.write("+OK\r\n".getBytes());
                outputStream.flush();
                break;
            }
          } else {
            outputStream.write("+OK\r\n".getBytes());
            outputStream.flush();
          }
          break;
        case "PSYNC": {
          String inputReplid = line[1];
          String inputOffset = line[2];
          if (role.equals("master")) {
            if (inputReplid.equals("?") && inputOffset.equals("-1")) {
              String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
              outputStream.write(simpleString("FULLRESYNC " + replid + " 0").getBytes());
              outputStream.flush();
              outputStream.write(("$" + emptyRDB.length + "\r\n").getBytes());
              outputStream.flush();
              outputStream.write(emptyRDB);
              outputStream.flush();
              replicaOutputStreams.add(outputStream);
            }
          }
          break;
        }
        default:
          outputStream.write(simpleError("ERR: Command " + command.toUpperCase() + " not found").getBytes());
          outputStream.flush();
          break;
      }
    }
  }

  static String simpleString(String input) {
    return "+" + input + "\r\n";
  }

  static String bulkString(String input) {
    return "$" + input.length() + "\r\n" + input + "\r\n";
  }

  static String simpleError(String input) {
    return "-" + input + "\r\n";
  }

  static String bulkStringArray(String[] input) {
    String returnString = "*" + input.length + "\r\n";
    for (String s : input) {
      returnString += bulkString(s);
    }
    return returnString;
  }

  static String readSimpleString(InputStream inputStream) {
    String s = "";
    try {
      char firstChar = (char) inputStream.read();
      if (firstChar == '+') {
        char nextChar;
        while ((nextChar = (char) inputStream.read()) != '\r') {
          s += nextChar;
        }
      } else {
        System.out.println(firstChar);
        throw new IllegalArgumentException("Simple string does not start with + (" + firstChar + ")");
      }
      inputStream.read(); // \n
      return s;
    } catch (IOException e) {
      return s;
    }
  }

  static String readBulkString(InputStream inputStream, boolean readEndBytes) {
    String s = "";
    try {
      char firstChar = (char) inputStream.read();
      if (firstChar == '$') {
        int length = 0;
        int digit = 0;
        while ((digit = inputStream.read()) != '\r') {
          length = length * 10 + (digit - '0');
        }

        inputStream.read(); // \n
        byte[] stringBytes = new byte[length];
        inputStream.read(stringBytes, 0, length);
        if (readEndBytes) {
          inputStream.read(); // \r
          inputStream.read(); // \n
        }
        return new String(stringBytes);
      } else {
        throw new IllegalArgumentException("Bulk string does not start with $ (" + firstChar + ")");
      }
    } catch (IOException e) {
      return s;
    }
  }
}
