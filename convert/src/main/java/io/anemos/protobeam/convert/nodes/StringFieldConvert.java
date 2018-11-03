package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class StringFieldConvert extends AbstractConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public String convert(Object in) {
        return (String) in;
    }

    @Override
    public void convert(Message message, TableRow row) {
        String fieldName = descriptor.getName();
        String value = convert(message.getField(descriptor));
        row.set(fieldName, value);
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, row.get(descriptor.getName()));
    }

    @Override
    public String convertFromGenericRecord(Object in) {
        return in.toString();
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, row.get(descriptor.getName()).toString());
    }
}
