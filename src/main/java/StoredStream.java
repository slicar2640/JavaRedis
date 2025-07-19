import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class StoredStream extends StoredValue {
  public ArrayList<HashMap<String, String>> entries = new ArrayList<>();
  private static final Pattern idRegex1 = Pattern.compile("\\d+-\\d+");
  private static final Pattern idRegex2 = Pattern.compile("\\d+-\\*");

  public StoredStream() {
    type = "stream";
  }

  public void addEntries(String id, HashMap<String, String> newEntries) throws Exception {
    if(id.equals("0-0")) {
      throw new Exception("ERR The ID specified in XADD must be greater than 0-0");
    }
    int format = idFormat(id);
    if (format == 1) {
      newEntries.put("id", id);
      entries.add(newEntries);
    } else if (format == 2) {

    } else if (format == 0) {

    } else {
      throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }
  }

  public String getOutput() {
    return "IDK";
  }

  int idFormat(String id) {
    if (id == "*") {
      return 0;
    } else if (idRegex1.matcher(id).matches()) {
      if (entries.size() == 0) {
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if(newMillis > 0) {
          return 1;
        } else {
          long newSequence = Long.valueOf(id.substring(id.indexOf('-') + 1));
          if(newSequence > 0) {
            return 1;
          } else {
            return -1;
          }
        }
      } else {
        String topId = entries.get(entries.size() - 1).get("id");
        long topMillis = Long.valueOf(topId.substring(0, topId.indexOf('-')));
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if (newMillis < topMillis) {
          return -1;
        } else if (newMillis == topMillis) {
          long topSequence = Long.valueOf(topId.substring(topId.indexOf('-') + 1));
          long newSequence = Long.valueOf(id.substring(id.indexOf('-') + 1));
          if (newSequence > topSequence) {
            return 1;
          } else {
            return -1;
          }
        } else {
          return 1;
        }
      }
    } else if (idRegex2.matcher(id).matches()) {
      String topId = entries.get(entries.size() - 1).get("id");
      long topMillis = Long.valueOf(topId.substring(0, topId.indexOf('-')));
      long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
      if(newMillis >= topMillis) {
        return 2;
      } else {
        return -1;
      }
    } else {
      return -1;
    }
  }
}
