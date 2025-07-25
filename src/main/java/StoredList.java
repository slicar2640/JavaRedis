import java.util.ArrayList;

class StoredList extends StoredValue {
  ArrayList<String> list = new ArrayList<>();

  public void push(String el) {
    list.add(el);
  }

  public int size() {
    return list.size();
  }
}
