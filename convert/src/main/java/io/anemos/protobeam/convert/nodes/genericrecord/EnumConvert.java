package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

class EnumConvert extends AbstractGenericRecordConvert<Object> {

    public EnumConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convertFrom(Object in) {
        Descriptors.EnumDescriptor enumType = fieldDescriptor.getEnumType();
        return enumType.findValueByName(((Utf8) in).toString());
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(fieldDescriptor, convertFrom(row.get(fieldDescriptor.getName())));
    }
}
