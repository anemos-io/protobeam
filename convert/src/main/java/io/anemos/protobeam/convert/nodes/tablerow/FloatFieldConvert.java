package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import java.util.Map;

class FloatFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
  public FloatFieldConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public void fromProto(Message message, TableRow row) {
    row.set(fieldDescriptor.getName(), fromProtoValue(message.getField(fieldDescriptor)));
  }

  @Override
  public Object fromProtoValue(Object in) {
    return ((Float) in).doubleValue();
  }

  @Override
  public Object toProtoValue(Object in) {
    return ((Double) in).floatValue();
  }

  @Override
  public void toProto(Map row, Message.Builder builder) {
    builder.setField(fieldDescriptor, toProtoValue(row.get(fieldDescriptor.getName())));
  }
}
