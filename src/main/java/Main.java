import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
  private static HashMap<String, String> storedData = new HashMap<>();

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
          int length = 0;
          int digit = 0;
          while ((digit = inputStream.read()) != '\r') {
            length = length * 10 + (digit - '0');
          }
          System.out.println(length);
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
          if (line.length == 3) {
            storedData.put(setKey, setValue);
          } else {
            if (line[3].toUpperCase().equals("PX")) {
              long expiry = Long.valueOf(line[4]);
              storeWithExpiry(setKey, setValue, expiry);
            }
          }
          outputWriter.write("+OK\r\n");
          outputWriter.flush();
          break;
        case "GET":
          System.out.println("GET");
          String getKey = line[1];
          System.out.println("key: " + getKey);
          String getValue = storedData.get(getKey);
          if (getValue == null) {
            outputWriter.write("$-1\r\n");
            outputWriter.flush();
            System.out.println("null");
          } else {
            System.out.println("$" + getValue.length() + "\r\n" + getValue + "\r\n");
            outputWriter.write("$" + getValue.length() + "\r\n" + getValue + "\r\n");
            outputWriter.flush();
          }
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

  static void storeWithExpiry(String key, String value, long expiryMillis) {
    storedData.put(key, value);
    Timer timer = new Timer();
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            storedData.remove(key);
            timer.cancel();
          }
        },
        expiryMillis);
  }
}
