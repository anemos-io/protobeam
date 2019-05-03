package io.anemos.protobeam.convert;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescriptorSerializer {

  private static void visitFileDescriptorTree(Map map, Descriptors.FileDescriptor fileDescriptor) {
    if (!map.containsKey(fileDescriptor.getName())) {
      map.put(fileDescriptor.getName(), fileDescriptor);
      List<Descriptors.FileDescriptor> dependencies = fileDescriptor.getDependencies();
      dependencies.forEach(fd -> visitFileDescriptorTree(map, fd));
    }
  }

  private static FileDescriptorSet createSet(Descriptors.FileDescriptor file) {
    HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    visitFileDescriptorTree(fileDescriptorMap, file);
    FileDescriptorSet.Builder builder = FileDescriptorSet.newBuilder();
    fileDescriptorMap.values().forEach(fd -> builder.addFile(fd.toProto()));
    return builder.build();
  }

  private static Map<String, FileDescriptorProto> extractProtoMap(
      FileDescriptorSet fileDescriptorSet) {
    HashMap<String, FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
    return map;
  }

  private static Descriptors.FileDescriptor getFileDescriptor(
      String name, FileDescriptorSet fileDescriptorSet) {
    Map<String, FileDescriptorProto> inMap = extractProtoMap(fileDescriptorSet);
    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();
    return convertToFileDescriptorMap(name, inMap, outMap);
  }

  private static Descriptors.FileDescriptor convertToFileDescriptorMap(
      String name,
      Map<String, FileDescriptorProto> inMap,
      Map<String, Descriptors.FileDescriptor> outMap) {
    if (outMap.containsKey(name)) {
      return outMap.get(name);
    }
    FileDescriptorProto fileDescriptorProto = inMap.get(name);
    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
    if (fileDescriptorProto.getDependencyCount() > 0) {
      fileDescriptorProto
          .getDependencyList()
          .forEach(
              dependencyName ->
                  dependencies.add(convertToFileDescriptorMap(dependencyName, inMap, outMap)));
    }
    try {
      Descriptors.FileDescriptor fileDescriptor =
          Descriptors.FileDescriptor.buildFrom(
              fileDescriptorProto, dependencies.toArray(new Descriptors.FileDescriptor[0]));
      outMap.put(name, fileDescriptor);
      return fileDescriptor;
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void writeObject(ObjectOutputStream oos, Descriptors.Descriptor descriptor)
      throws IOException {
    oos.writeUTF(descriptor.getFile().getName());
    oos.writeUTF(descriptor.getName());

    byte[] buffer = createSet(descriptor.getFile()).toByteArray();
    oos.writeInt(buffer.length);
    oos.write(buffer);

    // fieldDescriptor.getFile().
    // DescriptorProtos.FileDescriptorSet
    // HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();

  }

  public static Descriptors.Descriptor readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    String fileName = ois.readUTF();
    String typeName = ois.readUTF();

    byte[] buffer = new byte[ois.readInt()];
    ois.readFully(buffer);
    FileDescriptorSet fileDescriptorProto = FileDescriptorSet.parseFrom(buffer);
    Descriptors.FileDescriptor fileDescriptor = getFileDescriptor(fileName, fileDescriptorProto);

    for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {

      if (descriptor.getName().equals(typeName)) {
        return descriptor;
      }
    }
    return null;
  }
}
