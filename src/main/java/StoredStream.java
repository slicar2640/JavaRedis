import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class StoredStream extends StoredValue {
  public ArrayList<StreamEntry> entries = new ArrayList<>();
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
        entries.add(new StreamEntry(id, newEntries));
        return id;
      case TIME_AUTO:
        if (entries.size() > 0) {
          StreamEntry topElement = entries.get(entries.size() - 1);
          long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
          if (topElement.timeMillis == newMillis) {
            String correctId = id.substring(0, id.length() - 1) + (topElement.sequenceNum + 1);
            entries.add(new StreamEntry(correctId, newEntries));
            return correctId;
          } else {
            String correctId = id.substring(0, id.length() - 1) + (newMillis == 0 ? "1" : "0");
            entries.add(new StreamEntry(correctId, newEntries));
            return correctId;
          }
        } else {
          String withoutAsterisk = id.substring(0, id.length() - 1);
          String correctId = withoutAsterisk + (withoutAsterisk.equals("0-") ? 1 : 0);
          entries.add(new StreamEntry(correctId, newEntries));
          return correctId;
        }
      case AUTO:
        long millis = System.currentTimeMillis();
        if (entries.size() > 0) {
          StreamEntry topElement = entries.get(entries.size() - 1);
          if (millis == topElement.timeMillis) {
            String correctId = id.substring(0, id.length() - 1) + (topElement.sequenceNum + 1);
            entries.add(new StreamEntry(correctId, newEntries));
            return correctId;
          } else {
            String correctId = millis + (millis == 0 ? "1" : "0");
            entries.add(new StreamEntry(correctId, newEntries));
            return correctId;
          }
        } else {
          String correctId = millis + "-" + (millis == 0 ? "1" : "0");
          entries.add(new StreamEntry(correctId, newEntries));
          return correctId;
        }
      case INVALID:
        throw new RedisException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }
    return null;
  }

  IdFormat idFormat(String id) {
    if (id.equals("*")) {
      return IdFormat.AUTO;
    } else if (idRegex1.matcher(id).matches()) {
      if (entries.size() > 0) {
        StreamEntry topElement = entries.get(entries.size() - 1);
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if (newMillis < topElement.timeMillis) {
          return IdFormat.INVALID;
        } else if (newMillis == topElement.timeMillis) {
          long newSequence = Long.valueOf(id.substring(id.indexOf('-') + 1));
          if (newSequence > topElement.sequenceNum) {
            return IdFormat.TIME_SEQ;
          } else {
            return IdFormat.INVALID;
          }
        } else {
          return IdFormat.TIME_SEQ;
        }
      } else {
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
      }
    } else if (idRegex2.matcher(id).matches()) {
      if (entries.size() > 0) {
        StreamEntry topElement = entries.get(entries.size() - 1);
        long newMillis = Long.valueOf(id.substring(0, id.indexOf('-')));
        if (newMillis >= topElement.timeMillis) {
          return IdFormat.TIME_AUTO;
        } else {
          return IdFormat.INVALID;
        }
      } else {
        return IdFormat.TIME_AUTO;
      }
    } else {
      return IdFormat.INVALID;
    }
  }

  public ArrayList<StreamEntry> getRange(String start, String end) {
    int startIndex = 0;
    if (!start.equals("-")) {
      if (idRegex1.matcher(start).matches()) { // String id
        while (startIndex < entries.size()) {
          if (entries.get(startIndex).id.equals(start)) {
            break;
          }
          startIndex++;
        }
      } else { // Millis
        long startTime = Long.valueOf(start);
        while (startIndex < entries.size()) {
          if (entries.get(startIndex).timeMillis == startTime) {
            break;
          }
          startIndex++;
        }
      }
    }

    int endIndex = entries.size() - 1;
    if (!end.equals("+")) {
      if (idRegex1.matcher(end).matches()) { // String id
        while (endIndex >= 0) {
          if (entries.get(endIndex).id.equals(end)) {
            break;
          }
          endIndex--;
        }
      } else { // Millis
        long endTime = Long.valueOf(end);
        while (endIndex >= 0) {
          if (entries.get(endIndex).timeMillis == endTime) {
            break;
          }
          endIndex--;
        }
      }
    }

    return new ArrayList<StreamEntry>(entries.subList(startIndex, endIndex + 1));
  }

  public String getOutput() {
    return "Stream";
  }
}
