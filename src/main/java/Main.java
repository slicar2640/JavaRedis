import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
              BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(master.getOutputStream()));
              BufferedReader inputReader = new BufferedReader(new InputStreamReader(master.getInputStream()));
              outputWriter.write("*1\r\n$4\r\nPING\r\n");
              outputWriter.flush();
              inputReader.readLine();
              outputWriter
                  .write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n");
              outputWriter.flush();
              inputReader.readLine();
              outputWriter.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
              outputWriter.flush();
              inputReader.readLine();
              outputWriter.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
              outputWriter.flush();
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
        String[] line;
        int firstByte = inputStream.read();
        if (firstByte == -1)
          break; // client closed connection
        if ((char) firstByte == '*') {
          int length = 0;
          int digit = 0;
          while ((digit = inputStream.read()) != '\r') {
            length = length * 10 + (digit - '0');
          }
          inputStream.read(); // \n
          line = new String[length];
        } else {
          throw new Exception("Doesn't start with *");
        }

        for (int i = 0; i < line.length; i++) {
          char thisChar = (char) inputStream.read();
          if (thisChar == '$') {
            int length = 0;
            int digit = 0;
            while ((digit = inputStream.read()) != '\r') {
              length = length * 10 + (digit - '0');
            }

            inputStream.read(); // \n
            byte[] stringBytes = new byte[length];
            inputStream.read(stringBytes, 0, length);
            line[i] = new String(stringBytes);
            inputStream.read();
            inputStream.read(); // \r\n
          } else {
            System.out.println("thisChar: " + thisChar);
            throw new Exception("Not bulk string, probably should deal with this");
          }
        }
        parseCommand(inputStream, outputStream, line, transactionQueued, transaction);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println(e.toString());
    }
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
        BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
    String command = line[0];
    if (transactionQueued.value) {
      if (command.equalsIgnoreCase("EXEC")) {
        transactionQueued.value = false;
        String[] commandReturns = new String[transaction.size()];
        outputWriter.write("*" + commandReturns.length + "\r\n");
        outputWriter.flush();
        for (int i = 0; i < transaction.size(); i++) {
          parseCommand(inputStream, outputStream, transaction.get(i), new BooleanWrapper(false), null);
        }
        outputWriter.flush();
      } else if (command.equalsIgnoreCase("DISCARD")) {
        transaction.clear();
        transactionQueued.value = false;
        outputWriter.write("+OK\r\n");
        outputWriter.flush();
        return;
      } else {
        transaction.add(line);
        outputWriter.write(simpleString("QUEUED"));
        outputWriter.flush();
        return;
      }
    } else {
      switch (command.toUpperCase()) {
        case "PING": {
          outputWriter.write("+PONG\r\n");
          outputWriter.flush();
          break;
        }
        case "ECHO": {
          String toEcho = line[1];
          outputWriter.write("$" + toEcho.length() + "\r\n" + toEcho + "\r\n");
          outputWriter.flush();
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
          outputWriter.write("+OK\r\n");
          outputWriter.flush();
          break;
        }
        case "GET": {
          String key = line[1];
          StoredValue value = storedData.get(key);
          if (value == null) {
            outputWriter.write("$-1\r\n");
          } else if (value instanceof StoredString) {
            outputWriter.write(((StoredString) value).getOutput());
          } else {
            outputWriter.write("$-1\r\n");
          }
          outputWriter.flush();
          break;
        }
        case "TYPE": {
          String key = line[1];
          StoredValue value = storedData.get(key);
          if (value == null) {
            outputWriter.write("+none\r\n");
          } else {
            outputWriter.write(simpleString(value.type));
          }
          outputWriter.flush();
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
            outputWriter.write(bulkString(returnId));
            outputWriter.flush();
            break;
          } catch (RedisException e) {
            outputWriter.write(simpleError(e.getMessage()));
            outputWriter.flush();
            break;
          }
        }
        case "XRANGE": {
          String key = line[1];
          StoredStream stream = (StoredStream) storedData.get(key);
          ArrayList<StreamEntry> range = stream.getRange(line[2], line[3]);
          if (range.size() == 0) {
            outputWriter.write("$-1\r\n");
            outputWriter.flush();
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
          outputWriter.write(returnString);
          outputWriter.flush();
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
              outputWriter.write("$-1\r\n");
              outputWriter.flush();
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
          outputWriter.write(returnString);
          outputWriter.flush();
          break;
        }
        case "INCR": {
          String key = line[1];
          StoredValue storedValue = storedData.get(key);
          if (storedValue == null) {
            storedData.put(key, new StoredString("1"));
            outputWriter.write(":1\r\n");
          } else if (storedValue instanceof StoredString) {
            try {
              StoredString storedString = (StoredString) storedValue;
              int newVal = Integer.parseInt(storedString.value) + 1;
              storedString.value = String.valueOf(newVal);
              outputWriter.write(":" + newVal + "\r\n");
            } catch (NumberFormatException e) {
              outputWriter.write(simpleError("ERR value is not an integer or out of range"));
              outputWriter.flush();
              break;
            }
          } else {
            outputWriter.write(simpleError("Incremented value isn't a string"));
            outputWriter.flush();
            break;
          }
          outputWriter.flush();
          break;
        }
        case "MULTI": {
          transactionQueued.value = true;
          transaction.clear();
          outputWriter.write("+OK\r\n");
          outputWriter.flush();
          break;
        }
        case "EXEC": { // Won't ever be here if transactionQueued == true
          outputWriter.write(simpleError("ERR EXEC without MULTI"));
          outputWriter.flush();
          break;
        }
        case "DISCARD": { // Won't ever be here if transactionQueued == true
          outputWriter.write(simpleError("ERR DISCARD without MULTI"));
          outputWriter.flush();
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
                break;
              default:
                outputWriter.write(simpleError("ERR: Invalid section [" + section + "] for INFO command"));
                outputWriter.flush();
                break;
            }
          }
          outputWriter.write(bulkString(returnString));
          outputWriter.flush();
          break;
        }
        case "REPLCONF":
          outputWriter.write("+OK\r\n");
          outputWriter.flush();
          break;
        case "PSYNC": {
          String inputReplid = line[1];
          String inputOffset = line[2];
          if (role.equals("master")) {
            if (inputReplid.equals("?") && inputOffset.equals("-1")) {
              String replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
              outputWriter.write(simpleString("FULLRESYNC " + replid + " 0"));
              outputWriter.write(("$" + emptyRDB.length + "\r\n"));
              outputWriter.flush();
              outputStream.write(emptyRDB);
              outputStream.flush();
              break;
            }
          }
        }
        default:
          outputWriter.write(simpleError("ERR: Command " + command.toUpperCase() + " not found"));
          outputWriter.flush();
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
}
