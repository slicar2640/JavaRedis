import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Main {
  private static HashMap<String, StoredValue> storedData = new HashMap<>();

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    // System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("New client connected");
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
      String[] line;
      if ((char) inputStream.read() == '*') {
        int lengthByte = inputStream.read() - '0';
        line = new String[lengthByte];
        inputStream.read();
        inputStream.read(); // \r\n
      } else {
        throw new Exception("Doesn't start with *");
      }

      for (int i = 0; i < line.length; i++) {
        char thisChar = (char) inputStream.read();
        if (thisChar == '$') {
          int length = inputStream.read() - '0';
          byte[] stringBytes = new byte[length];
          inputStream.read();
          inputStream.read(); // \r\n
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
      switch (command.toUpperCase()) {
        case "PING":
          outputWriter.write("+PONG\r\n");
          outputWriter.flush();
          break;
        case "ECHO":
          String toEcho = line[1];
          outputWriter.write("$" + toEcho.length() + "\r\n" + toEcho + "\r\n");
          outputWriter.flush();
          break;
        case "SET":
          String setKey = line[1];
          String setValue = line[2];
          StoredValue stored = null;
          if (line.length == 3) {
            stored = new StoredValue(setValue);
          } else {
            if (line[3].toUpperCase().equals("PX")) {
              long expiry = Long.valueOf(line[4]);
              stored = new StoredValue(setValue, expiry);
            }
          }
          if (stored != null) {
            storedData.put(setKey, stored);
          }
          outputWriter.write("+OK\r\n");
            outputWriter.flush();
          break;
        case "GET":
          String getKey = line[1];
          StoredValue getStored = storedData.get(getKey);
          if (getStored == null) {
            outputWriter.write("$-1\r\n");
            outputWriter.flush();
            break;
          }
          String getValue = getStored.getValue();
          if (getValue != null) {
            outputWriter.write("$" + getValue.length() + "\r\n" + getValue + "\r\n");
          } else {
            outputWriter.write("$-1\r\n");
            storedData.remove(getKey);
            break;
          }
          outputWriter.flush();
          break;

        default:
          break;
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Exception: " + e.getMessage());
    }
  }
}
