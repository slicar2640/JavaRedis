import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Main {
  private HashMap<String, String> storedData = new HashMap<>();
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

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
        BufferedReader clientInput = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()));) {

      String content;
      while ((content = clientInput.readLine()) != null) {

        switch (content.toUpperCase()) {
          case "PING":
            outputWriter.write("+PONG\r\n");
            outputWriter.flush();
            break;
          case "ECHO":
            String numBytes = clientInput.readLine();
            String message = clientInput.readLine();
            outputWriter.write(numBytes + "\r\n" + message + "\r\n");
            outputWriter.flush();
            break;
          case "SET":
            while(true)System.out.println(clientInput.readLine());
            // break;

          default:
            break;
        }
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
