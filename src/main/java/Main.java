import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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

        String command = line[0];
        commandSwitch: switch (command.toUpperCase()) {
          case "PING": {
            outputWriter.write("+PONG\r\n");
            break;
          }
          case "ECHO": {
            String toEcho = line[1];
            outputWriter.write("$" + toEcho.length() + "\r\n" + toEcho + "\r\n");
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
            break;
          }
          case "GET": {
            String key = line[1];
            StoredValue value = storedData.get(key);
            if (value == null) {
              outputWriter.write("$-1\r\n");
            } else if(value instanceof StoredString) {
              outputWriter.write(((StoredString)value).getOutput());
            }
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
            } catch (RedisException e) {
              outputWriter.write(simpleError(e.getMessage()));
            }
            break;
          }
          case "XRANGE": {
            String key = line[1];
            StoredStream stream = (StoredStream) storedData.get(key);
            ArrayList<StreamEntry> range = stream.getRange(line[2], line[3]);
            if (range.size() == 0) {
              outputWriter.write("$-1\r\n");
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
                break commandSwitch;
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
            break;
          }
          case "INCR": {
            String key = line[1];
            StoredValue storedValue = storedData.get(key);
            if(storedValue == null) {
              storedData.put(key, new StoredString("1"));
              outputWriter.write(":1\r\n");
            } else if(storedValue instanceof StoredString) {
              try {
                StoredString storedString = (StoredString) storedValue;
                int newVal = Integer.parseInt(storedString.value) + 1;
                storedString.value = String.valueOf(newVal);
                outputWriter.write(":" + newVal + "\r\n");
              } catch(NumberFormatException e) {
                outputWriter.write(simpleError("ERR value is not an integer or out of range"));
              }
            }
            break;
          }
          default:
            break;
        }
        outputWriter.flush();
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
