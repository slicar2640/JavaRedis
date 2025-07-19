import java.util.HashMap;

public class StoredStream extends StoredValue {
  public HashMap<String, HashMap<String, String>> entries = new HashMap<>();
  public StoredStream() {
    type = "stream";
  }

  public void addEntries(String id, HashMap<String, String> newEntries) {
    entries.put(id, newEntries);
  }

  public String getOutput() {
    return "IDK";
  }
}
