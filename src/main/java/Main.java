import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

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
      System.out.println("length: " + line.length);
      for (int i = 0; i < line.length; i++) {
        if ((char) inputStream.read() == '$') {
          int length = inputStream.read();
          byte[] stringBytes = new byte[length];
          inputStream.read(stringBytes, 0, length);
          line[i] = new String(stringBytes);
          inputStream.read();
          inputStream.read(); // \r\n
        } else {
          throw new Exception("Not bulk string, probably should deal with this");
        }
      }
      for (String s : line)
        System.out.println(s);
      // while ((content = clientInput.readLine()) != null) {

      // switch (content.toUpperCase()) {
      // case "PING":
      // outputWriter.write("+PONG\r\n");
      // outputWriter.flush();
      // break;
      // case "ECHO":
      // String numBytes = clientInput.readLine();
      // String message = clientInput.readLine();
      // outputWriter.write(numBytes + "\r\n" + message + "\r\n");
      // outputWriter.flush();
      // break;
      // case "SET":
      // clientInput.readLine();
      // String setKey = clientInput.readLine();
      // String setValue = clientInput.readLine() + "\r\n" + clientInput.readLine() +
      // "\r\n";
      // System.out.println("Set " + setKey + " to " + setValue);
      // storedData.put(setKey, setValue);
      // outputWriter.write("+OK\r\n");
      // outputWriter.flush();
      // break;
      // case "GET":
      // clientInput.readLine();
      // String getKey = clientInput.readLine();
      // String getValue = storedData.get(getKey);
      // if(getValue == null) {
      // outputWriter.write("$-1\\r\\n");
      // } else {
      // outputWriter.write(getValue);
      // }
      // outputWriter.flush();
      // break;

      // default:
      // break;
      // }
      // }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Exception: " + e.getMessage());
    }
  }
}
