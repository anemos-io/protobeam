package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

public class IntegerFieldConvert extends AbstractConvert {
    public IntegerFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convert(Object in) {
        return ((Integer) in).longValue();
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public Object convertFromTableCell(Object in) {
        return ((Long) in).intValue();
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        builder.setField(descriptor, convertFromTableCell(row.get(descriptor.getName())));
    }

    @Override
    public Object convertFromGenericRecord(Object in) {
        return ((Long) in).intValue();
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, convertFromGenericRecord(row.get(descriptor.getName())));
    }
}
