package io.anemos.protobeam.test;

public class StringGenerator {

  private String[] data = new String[] {"12", "42", "69", "34", "0", "-123"};
  private int point = 0;

  public String nextString() {
    String val = data[point++];
    if (point >= data.length) point = 0;
    return val;
  }
}
