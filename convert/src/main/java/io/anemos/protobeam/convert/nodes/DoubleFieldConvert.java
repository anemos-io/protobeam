package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class DoubleFieldConvert extends AbstractConvert {
    public DoubleFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public Object convert(Object in) {
        return in;
    }

    @Override
    public Object convertFromTableCell(Object in) {
        return in;
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, convertFromTableCell(row.get(descriptor.getName())));
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        Double value = (Double) row.get(descriptor.getName());
        builder.setField(descriptor, convertFromGenericRecord(value));
    }
}
