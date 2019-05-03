package io.anemos.protobeam.test;

public class IntegerGenerator {

  private int[] data = new int[] {12, 42, 69, 34, 0, -123};
  private int point = 0;

  public int nextInt() {
    int val = data[point++];
    if (point >= data.length) point = 0;
    return val;
  }
}
