package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import java.util.Map;

class StringFieldConvert extends AbstractConvert<String, TableRow, Map<String, Object>> {
  public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public String fromProtoValue(Object in) {
    return (String) in;
  }

  @Override
  public void fromProto(Message message, TableRow row) {
    String fieldName = fieldDescriptor.getName();
    String value = fromProtoValue(message.getField(fieldDescriptor));
    row.set(fieldName, value);
  }

  @Override
  public void toProto(Map row, Message.Builder builder) {
    builder.setField(fieldDescriptor, row.get(fieldDescriptor.getName()));
  }
}
