package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.Row;

class RepeatedConvert extends AbstractBeamSqlConvert<Object> {

  private AbstractConvert field;

  public RepeatedConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
    super(descriptor);
    this.field = field;
  }

  @Override
  public void fromProto(Message message, Row.Builder row) {
    List list = new ArrayList<>();
    ((List) message.getField(fieldDescriptor)).forEach(obj -> list.add(field.fromProtoValue(obj)));
    row.addArray(list);
  }

  @Override
  public void toProto(Row row, Message.Builder builder) {
    List list = row.getArray(fieldDescriptor.getName());
    list.forEach(obj -> builder.addRepeatedField(fieldDescriptor, field.toProtoValue(obj)));
  }
}
