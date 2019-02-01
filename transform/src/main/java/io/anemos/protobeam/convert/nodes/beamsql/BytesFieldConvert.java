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
    public Object convert(Object in) {
        return ((ByteString) in).toByteArray();
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        byte[] bytes = row.getBytes(descriptor.getName());
        if (bytes.length > 0) {
            builder.setField(descriptor, bytes);
        }
    }
}
