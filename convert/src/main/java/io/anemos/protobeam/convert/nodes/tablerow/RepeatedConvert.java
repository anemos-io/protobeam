package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class RepeatedConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

  private AbstractConvert field;

  public RepeatedConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
    super(descriptor);
    this.field = field;
  }

  @Override
  public Object fromProtoValue(Object in) {
    return in;
  }

  @Override
  public void fromProto(Message message, TableRow row) {
    List tableCells = new ArrayList<>();
    ((List) message.getField(fieldDescriptor))
        .forEach(obj -> tableCells.add(field.fromProtoValue(obj)));
    row.set(fieldDescriptor.getName(), tableCells);
  }

  @Override
  public void toProto(Map row, Message.Builder builder) {
    List list = (List) row.get(fieldDescriptor.getName());
    list.forEach(obj -> builder.addRepeatedField(fieldDescriptor, field.toProtoValue(obj)));
  }
}
