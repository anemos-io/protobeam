package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class BytesFieldConvert extends AbstractBeamSqlConvert<Object> {
  public BytesFieldConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public Object fromProtoValue(Object in) {
    return ((ByteString) in).toByteArray();
  }

  @Override
  public void toProto(Row row, Message.Builder builder) {
    byte[] bytes = row.getBytes(fieldDescriptor.getName());
    if (bytes.length > 0) {
      builder.setField(fieldDescriptor, bytes);
    }
  }
}
