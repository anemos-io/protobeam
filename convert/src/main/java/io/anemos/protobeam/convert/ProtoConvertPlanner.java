package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class ProtoConvertPlanner implements Serializable {

  private Descriptors.Descriptor descriptor;

  private ConvertNodeFactory nodeFactory;

  private SchemaProtoContext context = new SchemaProtoContext();

  ProtoConvertPlanner(Descriptors.Descriptor descriptor, ConvertNodeFactory factory) {
    this.nodeFactory = factory;
    this.descriptor = descriptor;
  }

  public String getNodeFactoryClassName() {
    return nodeFactory.getClass().getName();
  }

  private AbstractConvert planField(Descriptors.FieldDescriptor fieldDescriptor) {

    AbstractConvert nextConvert;
    Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();
    switch (fieldType) {
      case BOOL:
        nextConvert = nodeFactory.createBooleanFieldConvert(fieldDescriptor);
        break;
      case INT64:
      case UINT64:
      case FIXED64:
      case SFIXED64:
      case SINT64:
        nextConvert = nodeFactory.createLongFieldConvert(fieldDescriptor);
        break;
      case INT32:
      case UINT32:
      case SFIXED32:
      case FIXED32:
      case SINT32:
        nextConvert = nodeFactory.createIntegerFieldConvert(fieldDescriptor);
        break;
      case DOUBLE:
        nextConvert = nodeFactory.createDoubleFieldConvert(fieldDescriptor);
        break;
      case FLOAT:
        nextConvert = nodeFactory.createFloatFieldConvert(fieldDescriptor);
        break;
      case BYTES:
        nextConvert = nodeFactory.createBytesFieldConvert(fieldDescriptor);
        break;
      case STRING:
        nextConvert = nodeFactory.createStringFieldConvert(fieldDescriptor);
        break;
      case ENUM:
        nextConvert = nodeFactory.createEnumFieldConvert(fieldDescriptor);
        break;
      case MESSAGE:
        nextConvert = planMessageField(fieldDescriptor);
        break;
      default:
        throw new RuntimeException("Field type " + fieldType.toString() + " not supported");
    }
    if (context.hasRenameTo(fieldDescriptor)) {
      String newName = context.getRenameTo(fieldDescriptor);
      return nodeFactory.createRenameFieldConvert(newName, fieldDescriptor, nextConvert);
    }
    return nextConvert;
  }

  private AbstractConvert planMessageField(Descriptors.FieldDescriptor fieldDescriptor) {
    AbstractConvert nextConvert;
    if (context.isTimestamp(fieldDescriptor)) {
      nextConvert = nodeFactory.createWktTimestampFieldConvert(fieldDescriptor);
    } else if (context.flatten(fieldDescriptor)) {
      nextConvert =
          nodeFactory.createFlattenFieldConvert(
              fieldDescriptor,
              planMessage(fieldDescriptor, FieldExplorer.of(fieldDescriptor.getMessageType())));
    } else if (context.isWrapper(fieldDescriptor)) {
      nextConvert = nodeFactory.createWktWrapperFieldConvert(fieldDescriptor);
    } else {
      nextConvert =
          nodeFactory.createMessageFieldConvert(
              fieldDescriptor,
              planMessage(fieldDescriptor, FieldExplorer.of(fieldDescriptor.getMessageType())));
    }
    return nextConvert;
  }

  private AbstractMessageConvert planMessage(
      Descriptors.FieldDescriptor fieldDescriptor, FieldExplorer fields) {
    List<AbstractConvert> list = new ArrayList<>();
    fields.forEach(
        field -> {
          Descriptors.FieldDescriptor fd = field.fieldDescriptor;
          if (fd.isRepeated()) {
            list.add(nodeFactory.createRepeatedFieldConvert(fd, planField(fd)));
          } else if (field.isOneOf) {
            list.add(nodeFactory.createNullableFieldConvert(fd, planField(fd)));
          } else {
            list.add(planField(fd));
          }
        });

    if (context.hasNonDefaultTimePartitioningTruncate(fields.parentDescriptor)) {
      return nodeFactory.createPartitionColumnConvert(
          fields.parentDescriptor, nodeFactory.createMessageConvert(null, list));
    }
    return nodeFactory.createMessageConvert(null, list);
  }

  AbstractConvert createPlan() {
    return planMessage(null, FieldExplorer.of(descriptor));
  }
}
