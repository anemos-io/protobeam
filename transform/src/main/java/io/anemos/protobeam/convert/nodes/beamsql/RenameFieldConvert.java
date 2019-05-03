package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.beam.sdk.values.Row;

public class RenameFieldConvert extends AbstractBeamSqlConvert<Object> {

  AbstractConvert convert;
  Descriptors.FieldDescriptor descriptor;
  String renameTo;

  // does nothing, only schema needs to be adjusted for a rename.
  public RenameFieldConvert(
      String renameTo, Descriptors.FieldDescriptor descriptor, AbstractConvert convert) {
    super(descriptor);
    this.convert = convert;
    this.descriptor = descriptor;
    this.renameTo = renameTo;
  }

  @Override
  public Object fromProtoValue(Object in) {
    return in;
  }

  @Override
  public void toProto(Row row, Message.Builder builder) {
    Object value = row.getValue(renameTo);
    //        convert.toProto(row, builder);
    builder.setField(descriptor, value);
  }
}
