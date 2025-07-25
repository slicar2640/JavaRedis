import java.util.ArrayList;

class StoredList extends StoredValue {
  ArrayList<String> list = new ArrayList<>();

  public void push(String el) {
    list.add(el);
  }

  public int size() {
    return list.size();
  }

  public ArrayList<String> subList(int firstIndex, int secondIndex) {
    if(firstIndex >= list.size()) {
      return new ArrayList<>();
    }
    return new ArrayList<>(list.subList(firstIndex, Math.min(secondIndex, list.size())));
  }
}
