package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;

class BytesFieldConvert extends AbstractGenericRecordConvert<Object> {
  public BytesFieldConvert(Descriptors.FieldDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public void toProto(GenericRecord row, Message.Builder builder) {
    ByteBuffer bb = (ByteBuffer) row.get(fieldDescriptor.getName());
    byte[] bytes = bb.array();
    if (bytes.length > 0) {
      builder.setField(fieldDescriptor, bytes);
    }
  }
}
