package io.anemos.protobeam.test;

public class FloatGenerator {

  private float[] data = new float[] {12.0f, 42f, 69f, 34f, 0f, -123f};
  private int point = 0;

  public float nextFloat() {
    float val = data[point++];
    if (point >= data.length) point = 0;
    return val;
  }
}
