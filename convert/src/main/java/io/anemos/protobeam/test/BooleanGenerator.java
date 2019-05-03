package io.anemos.protobeam.test;

public class BooleanGenerator {

  private boolean[] data = new boolean[] {true, false};
  private int point = 0;

  public boolean nextBoolean() {
    boolean val = data[point++];
    if (point >= data.length) point = 0;
    return val;
  }
}
