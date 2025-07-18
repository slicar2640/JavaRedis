public class StoredValue {
  private long expiry;
  private long declaredMillis;
  private String value;
  public StoredValue(String val) {
    value = new String(val);
    expiry = -1;
  }

  public StoredValue(String val, long expiry) {
    value = new String(val);
    this.expiry = expiry;
    declaredMillis = System.currentTimeMillis();
  }

  public String getValue() {
    if(expiry == -1 || System.currentTimeMillis() <= declaredMillis + expiry) {
      return value;
    } else {
      return null;
    }
  }
}
