package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

class MessageFieldConvert extends AbstractGenericRecordConvert<Object> {
  AbstractMessageConvert convert;

  public MessageFieldConvert(
      Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
    super(descriptor);
    this.convert = convert;
  }

  @Override
  public void toProto(GenericRecord row, Message.Builder builder) {
    GenericData.Record nested = (GenericData.Record) row.get(fieldDescriptor.getName());
    if (nested != null) {
      DynamicMessage.Builder fieldBuilder =
          DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
      convert.toProto(nested, fieldBuilder);
      builder.setField(fieldDescriptor, fieldBuilder.build());
    }
  }
}
