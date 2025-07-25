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
    if (firstIndex < 0) {
      firstIndex += list.size();
    }
    firstIndex = Math.clamp(firstIndex, 0, list.size() - 1);

    if (secondIndex < 0) {
      secondIndex += list.size();
    }
    secondIndex = Math.clamp(secondIndex, 0, list.size() - 1);

    System.out.println(firstIndex + " " + secondIndex);
    return new ArrayList<>(list.subList(firstIndex, secondIndex + 1));
  }
}
