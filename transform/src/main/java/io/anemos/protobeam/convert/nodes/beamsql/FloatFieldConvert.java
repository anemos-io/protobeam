package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class FloatFieldConvert extends AbstractBeamSqlConvert<Object> {
  public FloatFieldConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public void toProto(Row row, Message.Builder builder) {
    builder.setField(fieldDescriptor, toProtoValue(row.getFloat(fieldDescriptor.getName())));
  }
}
