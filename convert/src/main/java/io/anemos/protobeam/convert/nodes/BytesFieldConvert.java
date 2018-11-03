package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

public class BytesFieldConvert extends AbstractConvert {
    private Base64.Decoder decoder = Base64.getDecoder();
    private Base64.Encoder encoder = Base64.getEncoder();

    public BytesFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convert(Object in) {
        return encoder.encodeToString(((ByteString) in).toByteArray());
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public Object convertFromTableCell(Object in) {
        return decoder.decode((String) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {

        byte[] bytes = (byte[]) convertFromTableCell(row.get(descriptor.getName()));
        if (bytes != null && bytes.length > 0) {
            builder.setField(descriptor, bytes);
        }
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
