package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class WktWrapperConvert extends AbstractBeamSqlConvert<Object> {

  private Descriptors.FieldDescriptor valueFieldDescriptor;

  public WktWrapperConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
    this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
  }

  @Override
  public void fromProto(Message message, Row.Builder row) {
    if (message.hasField(fieldDescriptor)) {
      Message valueMessage = (Message) message.getField(fieldDescriptor);
      Object value = fromProtoValue(valueMessage.getField(valueFieldDescriptor));
      row.addValue(value);
    } else {
      row.addValue(null);
    }
  }

  @Override
  public void toProto(Row row, Message.Builder builder) {
    Object value = row.getValue(fieldDescriptor.getName());
    if (null != value) {
      DynamicMessage wrapperMessage =
          DynamicMessage.newBuilder(fieldDescriptor.getMessageType())
              .setField(valueFieldDescriptor, value)
              .build();
      builder.setField(fieldDescriptor, wrapperMessage);
    }
  }
}
