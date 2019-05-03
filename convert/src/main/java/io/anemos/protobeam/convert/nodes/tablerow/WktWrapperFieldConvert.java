package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import java.util.Map;

class WktWrapperFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

  private Descriptors.FieldDescriptor valueFieldDescriptor;

  public WktWrapperFieldConvert(Descriptors.FieldDescriptor fieldDescriptor) {
    super(fieldDescriptor);
    this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
  }

  @Override
  public Object fromProtoValue(Object in) {
    return in;
  }

  @Override
  public void fromProto(Message message, TableRow row) {
    if (message.hasField(fieldDescriptor)) {
      Message valueMessage = (Message) message.getField(fieldDescriptor);
      Object value = fromProtoValue(valueMessage.getField(valueFieldDescriptor));
      row.set(fieldDescriptor.getName(), value);
    }
  }

  @Override
  public void toProto(Map<String, Object> row, Message.Builder builder) {
    if (row.containsKey(fieldDescriptor.getName())) {
      Object obj = row.get(fieldDescriptor.getName());
      DynamicMessage wrapperMessage =
          DynamicMessage.newBuilder(fieldDescriptor.getMessageType())
              .setField(valueFieldDescriptor, obj)
              .build();
      builder.setField(fieldDescriptor, wrapperMessage);
    }
  }
}
