import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
  private static HashMap<String, StoredValue> storedData = new HashMap<>();

  public static void main(String[] args) {

    ServerSocket serverSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  static void handleClient(Socket clientSocket) {
    try (clientSocket; // automatically closes socket at the end
        BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
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
          int length = readInteger(inputStream);
          line = new String[length];
        } else {
          throw new Exception("Doesn't start with *");
        }

        for (int i = 0; i < line.length; i++) {
          line[i] = readBulkString(inputStream);
        }
        parseCommand(line, transactionQueued, transaction, outputWriter);
      }
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

  static void parseCommand(String[] line, BooleanWrapper transactionQueued, ArrayList<String[]> transaction,
      BufferedWriter outputWriter)
      throws InterruptedException, IOException {
    String command = line[0];
    if (transactionQueued.value) {
      if (command.equalsIgnoreCase("EXEC")) {
        transactionQueued.value = false;
        outputWriter.write("*" + transaction.size() + "\r\n");
        for (int i = 0; i < transaction.size(); i++) {
          parseCommand(transaction.get(i), new BooleanWrapper(false), null, outputWriter);
        }
      } else if (command.equalsIgnoreCase("DISCARD")) {
        transaction.clear();
        transactionQueued.value = false;
        outputWriter.write("+OK\r\n");
      } else {
        transaction.add(line);
        outputWriter.write(simpleString("QUEUED"));
      }
      outputWriter.flush();
      return;
    } else {
      switch (command.toUpperCase()) {
        case "PING": {
          outputWriter.write("+PONG\r\n");
          outputWriter.flush();
          return;
        }
        case "ECHO": {
          String toEcho = line[1];
          outputWriter.write("$" + toEcho.length() + "\r\n" + toEcho + "\r\n");
          outputWriter.flush();
          return;
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
          return;
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
          return;
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
          return;
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
          } catch (RedisException e) {
            outputWriter.write(simpleError(e.getMessage()));
          }
          outputWriter.flush();
          return;
        }
        case "XRANGE": {
          String key = line[1];
          StoredStream stream = (StoredStream) storedData.get(key);
          ArrayList<StreamEntry> range = stream.getRange(line[2], line[3]);
          if (range.size() == 0) {
            outputWriter.write("$-1\r\n");
            outputWriter.flush();
            return;
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
          return;
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
              return;
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
          return;
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
            }
          } else {
            outputWriter.write(simpleError("Incremented value isn't a string"));
          }
          outputWriter.flush();
          return;
        }
        case "MULTI": {
          transactionQueued.value = true;
          transaction.clear();
          outputWriter.write("+OK\r\n");
          outputWriter.flush();
          return;
        }
        case "EXEC": { // Won't ever be here if transactionQueued == true
          outputWriter.write(simpleError("ERR EXEC without MULTI"));
          outputWriter.flush();
          return;
        }
        case "DISCARD": { // Won't ever be here if transactionQueued == true
          outputWriter.write(simpleError("ERR DISCARD without MULTI"));
          outputWriter.flush();
          return;
        }
        case "RPUSH": {
          String key = line[1];
          if (!storedData.containsKey(key)) {
            storedData.put(key, new StoredList());
          }
          StoredList storedList = (StoredList) storedData.get(key);
          for (int i = 2; i < line.length; i++) {
            String element = line[i];
            storedList.push(element);
          }
          outputWriter.write(redisInteger(storedList.size()));
          outputWriter.flush();
          return;
        }
        case "LPUSH": {
          String key = line[1];
          if (!storedData.containsKey(key)) {
            storedData.put(key, new StoredList());
          }
          StoredList storedList = (StoredList) storedData.get(key);
          for (int i = 2; i < line.length; i++) {
            String element = line[i];
            storedList.prepush(element);
          }
          outputWriter.write(redisInteger(storedList.size()));
          outputWriter.flush();
          return;
        }
        case "LRANGE": {
          String key = line[1];
          int firstIndex = Integer.parseInt(line[2]);
          int secondIndex = Integer.parseInt(line[3]);
          if (storedData.containsKey(key)) {
            StoredList storedList = (StoredList) storedData.get(key);
            ArrayList<String> subList = storedList.subList(firstIndex, secondIndex);
            outputWriter.write(bulkStringArray(subList));
          } else {
            outputWriter.write("*0\r\n");
          }
          outputWriter.flush();
          return;
        }
        case "LLEN": {
          String key = line[1];
          if (storedData.containsKey(key)) {
            StoredList storedList = (StoredList) storedData.get(key);
            outputWriter.write(redisInteger(storedList.size()));
          } else {
            outputWriter.write(":0\r\n");
          }
          outputWriter.flush();
          return;
        }
        case "LPOP": {
          String key = line[1];
          if (storedData.containsKey(key)) {
            StoredList storedList = (StoredList) storedData.get(key);
            if (storedList.size() == 0) {
              outputWriter.write("$-1\r\n");
              outputWriter.flush();
              return;
            }
            if (line.length == 2) {
              outputWriter.write(bulkString(storedList.popFirst()));
            } else {
              int count = Integer.parseInt(line[2]);
              outputWriter.write(bulkStringArray(storedList.popFirst(count)));
            }
          } else {
            outputWriter.write("$-1\r\n");
          }
          outputWriter.flush();
          return;
        }
        case "BLPOP": {
          double blockTime = Double.parseDouble(line[line.length - 1]);
          double startTime = System.currentTimeMillis();
          // BLOCK 0: wait indefinitely for data
          ArrayList<String> waiting = new ArrayList<String>(
              Arrays.asList(Arrays.copyOfRange(line, 1, line.length - 1)));
          while (waiting.size() > 0 && !(blockTime > 0 && System.currentTimeMillis() - startTime > blockTime * 1000)) {
            ArrayList<String> toRemove = new ArrayList<>();
            for (String listKey : waiting) {
              if (storedData.containsKey(listKey)) {
                StoredList storedList = (StoredList) storedData.get(listKey);
                if (storedList.size() > 0) {
                  Thread.sleep(11); // This is so hacky but IDC I'm done
                  if (storedList.size() > 0) {
                    outputWriter.write(bulkStringArray(listKey, storedList.popFirst()));
                    outputWriter.flush();
                    toRemove.add(listKey);
                  }
                }
              }
            }
            waiting.removeAll(toRemove);
          }
          if (waiting.size() > 0) {
            outputWriter.write("$-1\r\n");
            outputWriter.flush();
          }
          return;
        }
        default:
          outputWriter.write(simpleError("ERR: Command " + command.toUpperCase() + " not found"));
          outputWriter.flush();
          return;
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

  static String redisInteger(int input) {
    return ":" + Integer.toString(input) + "\r\n";
  }

  static String bulkStringArray(String... input) {
    String returnString = "*" + input.length + "\r\n";
    for (String element : input) {
      returnString += bulkString(element);
    }
    return returnString;
  }

  static String bulkStringArray(ArrayList<String> input) {
    String returnString = "*" + input.size() + "\r\n";
    for (String element : input) {
      returnString += bulkString(element);
    }
    return returnString;
  }

  static int readInteger(InputStream inputStream) throws IOException {
    int length = 0;
    int digit = 0;
    while ((digit = inputStream.read()) != '\r') {
      length = length * 10 + (digit - '0');
    }
    inputStream.read(); // \n
    return length;
  }

  static String readBulkString(InputStream inputStream) throws IOException, IllegalArgumentException {
    char thisChar = (char) inputStream.read();
    if (thisChar == '$') {
      int length = readInteger(inputStream);
      byte[] stringBytes = new byte[length];
      inputStream.read(stringBytes, 0, length);
      inputStream.read(); // \r
      inputStream.read(); // \n
      return new String(stringBytes);
    } else {
      throw new IllegalArgumentException("Bulk string does not start with $ (" + thisChar + ")");
    }
  }
}
