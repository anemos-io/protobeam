package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class EnumConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

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
    public Object convertFrom(Object in) {
        Descriptors.EnumDescriptor enumType = descriptor.getEnumType();
        return enumType.findValueByName((String) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, convertFrom(row.get(descriptor.getName())));
    }
}
