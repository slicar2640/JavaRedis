import java.util.ArrayList;

class StoredList extends StoredValue {
  ArrayList<String> list = new ArrayList<>();

  public int push(String el) {
    list.add(el);
    return list.size();
  }
}
