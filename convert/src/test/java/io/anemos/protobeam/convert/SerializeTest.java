package io.anemos.protobeam.convert;

import java.io.*;

public class SerializeTest {

  public static byte[] serializeToByteArray(Serializable value) {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
        oos.writeObject(value);
      }
      return buffer.toByteArray();
    } catch (IOException exn) {
      throw new IllegalArgumentException("unable to serialize " + value, exn);
    }
  }

  public static Object deserializeFromByteArray(byte[] encodedValue, String description) {
    try {
      try (ObjectInputStream ois =
          new ObjectInputStream((new ByteArrayInputStream(encodedValue)))) {
        return ois.readObject();
      }
    } catch (IOException | ClassNotFoundException exn) {
      throw new IllegalArgumentException("unable to deserialize " + description, exn);
    }
  }
}
