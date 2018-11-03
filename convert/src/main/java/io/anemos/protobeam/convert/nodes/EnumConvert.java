package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.Map;

public class EnumConvert extends AbstractConvert {

    public EnumConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convert(Object in) {
        Descriptors.EnumValueDescriptor enumValue = ((Descriptors.EnumValueDescriptor) in);
        return enumValue.getName();
        // TODO what with            if (!"__UNDEFINED_0".equals(value))
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public Object convertFromTableCell(Object in) {
        Descriptors.EnumDescriptor enumType = descriptor.getEnumType();
        return enumType.findValueByName((String) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, convertFromTableCell(row.get(descriptor.getName())));
    }

    @Override
    public Object convertFromGenericRecord(Object in) {
        Descriptors.EnumDescriptor enumType = descriptor.getEnumType();
        return enumType.findValueByName(((Utf8) in).toString());
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, convertFromGenericRecord(row.get(descriptor.getName())));
    }
}
