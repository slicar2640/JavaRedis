public class StoredString extends StoredValue {
  String value;

  public StoredString(String val) {
    type = "string";
    value = new String(val);
  }

  public String getOutput() {
    return Main.bulkString(value);
  }
}
