package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.avro.generic.GenericRecord;

public class FlattenFieldConvert extends AbstractGenericRecordConvert<Object> {

  private AbstractMessageConvert convert;

  FlattenFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
    super(descriptor);
    this.convert = convert;
  }

  @Override
  public void toProto(GenericRecord row, Message.Builder builder) {
    DynamicMessage.Builder dynamicMessageBuilder =
        DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
    fieldDescriptor
        .getMessageType()
        .getFields()
        .forEach(
            subFieldDescriptor -> {
              Object value = row.get(subFieldDescriptor.getName());
              dynamicMessageBuilder.setField(subFieldDescriptor, value);
            });
    builder.setField(fieldDescriptor, dynamicMessageBuilder.build());
  }
}
