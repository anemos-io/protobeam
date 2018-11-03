package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class LongFieldConvert extends AbstractConvert {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convert(Object in) {
        return in.toString();
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, convertFromTableCell(row.get(descriptor.getName())));
    }

    @Override
    public Object convertFromTableCell(Object in) {
        return Long.parseLong((String) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        Long value = (Long) row.get(descriptor.getName());
        builder.setField(descriptor, convertFromGenericRecord(value));
    }
}
