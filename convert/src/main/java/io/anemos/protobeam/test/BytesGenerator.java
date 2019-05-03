package io.anemos.protobeam.test;

public class BytesGenerator {

  private byte[][] data =
      new byte[][] {
        new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE},
        new byte[] {},
        new byte[] {(byte) 0x00},
        new byte[] {(byte) 0xFF},
      };
  private int point = 0;

  public byte[] nextBytes() {
    byte[] val = data[point++];
    if (point >= data.length) point = 0;
    return val;
  }
}
