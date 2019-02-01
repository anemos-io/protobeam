package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

class BytesFieldConvert extends AbstractGenericRecordConvert<Object> {
    public BytesFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        ByteBuffer bb = (ByteBuffer) row.get(descriptor.getName());
        byte[] bytes = bb.array();
        if (bytes.length > 0) {
            builder.setField(descriptor, bytes);
        }
    }
}
