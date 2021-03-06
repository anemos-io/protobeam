package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import java.util.List;

public interface ConvertNodeFactory {

  AbstractConvert createBooleanFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createBytesFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createDoubleFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createEnumFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createFlattenFieldConvert(
      Descriptors.FieldDescriptor fieldDescriptor, AbstractMessageConvert convert);

  AbstractConvert createFloatFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createIntegerFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createLongFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createMessageFieldConvert(
      Descriptors.FieldDescriptor fieldDescriptor, AbstractMessageConvert convert);

  AbstractMessageConvert createMessageConvert(
      Descriptors.FieldDescriptor fieldDescriptor, List<AbstractConvert> fields);

  AbstractConvert createNullableFieldConvert(
      Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field);

  AbstractMessageConvert createPartitionColumnConvert(
      Descriptors.Descriptor messageDescriptor, AbstractMessageConvert convert);

  AbstractConvert createRenameFieldConvert(
      String newName, Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field);

  AbstractConvert createRepeatedFieldConvert(
      Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field);

  AbstractConvert createStringFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createWktTimestampFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);

  AbstractConvert createWktWrapperFieldConvert(Descriptors.FieldDescriptor fieldDescriptor);
}
