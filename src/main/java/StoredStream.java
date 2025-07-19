import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class StoredStream extends StoredValue {
  public ArrayList<HashMap<String, String>> entries = new ArrayList<>();
  private static final Pattern idRegex1 = Pattern.compile("\\d+-\\d+");
  private static final Pattern idRegex2 = Pattern.compile("\\d+-\\*");

  private static enum IdFormat {
    INVALID,
    AUTO,
    TIME_SEQ,
    TIME_AUTO
  }

  public StoredStream() {
    type = "stream";
  }

  public String addEntries(String id, HashMap<String, String> newEntries) throws RedisException {
    if (id.equals("0-0")) {
      throw new RedisException("ERR The ID specified in XADD must be greater than 0-0");
    }
    IdFormat format = idFormat(id);
    switch (format) {
      case TIME_SEQ:
        newEntries.put("id", id);
        entries.add(newEntries);
        return id;
      case TIME_AUTO:
        if (entries.size() > 0) {
          String topId = entries.get(entries.size() - 1).get("id");
          String topMillis = topId.substring(0, topId.indexOf('-'));
          String newMillis = id.substring(0, id.indexOf('-'));
          if (topMillis.equals(newMillis)) {
            long topSequence = Long.valueOf(topId.substring(topId.indexOf('-') + 1));
            String correctId = id.substring(0, id.length() - 1) + (topSequence + 1);
            newEntries.put("id", correctId);
            entries.add(newEntries);
            return correctId;
          } else {
            String correctId = id.substring(0, id.length() - 1) + (newMillis.equals("0") ? "1" : "0");
            newEntries.put("id", correctId);
            entries.add(newEntries);
            return correctId;
          }
        } else {
          String millis = id.substring(0, id.length() - 1);
          String correctId = millis + (millis.equals("0-") ? 1 : 0);
          newEntries.put("id", correctId);
          entries.add(newEntries);
          return correctId;
        }
      case AUTO:
        String millis = Long.toString(System.currentTimeMillis());
        if (entries.size() > 0) {
          String topId = entries.get(entries.size() - 1).get("id");
          String topMillis = topId.substring(0, topId.indexOf('-'));
          if (millis.equals(topMillis)) {
            long topSequence = Long.valueOf(topId.substring(topId.indexOf('-') + 1));
            String correctId = id.substring(0, id.length() - 1) + (topSequence + 1);
            newEntries.put("id", correctId);
            entries.add(newEntries);
            return correctId;
          } else {
            String correctId = millis + (millis.equals("0-") ? 1 : 0);
            newEntries.put("id", correctId);
            entries.add(newEntries);
            return correctId;
          }
        } else {
          String correctId = millis + "-" + (millis.equals("0") ? 1 : 0);
          newEntries.put("id", correctId);
          entries.add(newEntries);
          return correctId;
        }
      case INVALID:
        throw new RedisException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }
    return null;
  }

  public String getOutput() {
    return "IDK";
  }

  IdFormat idFormat(String id) {
    if (id.equals("*")) {
      return IdFormat.AUTO;
    } else if (idRegex1.matcher(id).matches()) {
      if (entries.size() == 0) {
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if (newMillis > 0) {
          return IdFormat.TIME_SEQ;
        } else {
          long newSequence = Long.valueOf(id.substring(id.indexOf('-') + 1));
          if (newSequence > 0) {
            return IdFormat.TIME_SEQ;
          } else {
            return IdFormat.INVALID;
          }
        }
      } else {
        String topId = entries.get(entries.size() - 1).get("id");
        long topMillis = Long.valueOf(topId.substring(0, topId.indexOf('-')));
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if (newMillis < topMillis) {
          return IdFormat.INVALID;
        } else if (newMillis == topMillis) {
          long topSequence = Long.valueOf(topId.substring(topId.indexOf('-') + 1));
          long newSequence = Long.valueOf(id.substring(id.indexOf('-') + 1));
          if (newSequence > topSequence) {
            return IdFormat.TIME_SEQ;
          } else {
            return IdFormat.INVALID;
          }
        } else {
          return IdFormat.TIME_SEQ;
        }
      }
    } else if (idRegex2.matcher(id).matches()) {
      String topId = entries.size() == 0 ? "0-0" : entries.get(entries.size() - 1).get("id");
      long topMillis = Long.valueOf(topId.substring(0, topId.indexOf('-')));
      long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
      if (newMillis >= topMillis) {
        return IdFormat.TIME_AUTO;
      } else {
        return IdFormat.INVALID;
      }
    } else {
      return IdFormat.INVALID;
    }
  }
}
