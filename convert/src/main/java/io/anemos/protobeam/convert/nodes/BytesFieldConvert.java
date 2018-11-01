package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

public class BytesFieldConvert extends AbstractConvert {
    public BytesFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), ((ByteString) message.getField(descriptor)).toByteArray());
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        byte[] bytes = (byte[]) row.get(descriptor.getName());
        if (bytes != null && bytes.length > 0) {
            builder.setField(descriptor, bytes);
        }
    }
}
