import java.util.HashMap;

public class StreamEntry {
  public String id;
  public long timeMillis;
  public long sequenceNum;
  public HashMap<String, String> values = new HashMap<>();

  public StreamEntry(String id, HashMap<String, String> values) {
    this.id = id;
    timeMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
    sequenceNum = Long.valueOf(id.substring(id.indexOf('-') + 1));
    this.values = values;
  }
}
