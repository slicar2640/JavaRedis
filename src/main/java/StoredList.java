import java.util.ArrayList;

class StoredList extends StoredValue {
  ArrayList<String> list = new ArrayList<>();

  public void push(String el) {
    list.add(el);
  }

  public void prepush(String el) {
    list.add(0, el);
  }

  public int size() {
    return list.size();
  }

  public ArrayList<String> subList(int firstIndex, int secondIndex) {
    if (firstIndex < 0) {
      firstIndex += list.size();
    }
    firstIndex = Math.clamp(firstIndex, 0, list.size() - 1);

    if (secondIndex < 0) {
      secondIndex += list.size();
    }
    secondIndex = Math.clamp(secondIndex, 0, list.size() - 1);

    return new ArrayList<>(list.subList(firstIndex, secondIndex + 1));
  }

  public String popFirst() {
    return list.remove(0);
  }

  public String[] popFirst(int count) {
    String[] popped = new String[count];
    for(int i = 0; i < count; i++) {
      popped[i] = list.remove(0);
    }
    return popped;
  }
}
