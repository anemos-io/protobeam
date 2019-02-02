package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class IntegerFieldConvert extends AbstractGenericRecordConvert<Object> {
    public IntegerFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convertFrom(Object in) {
        return ((Long) in).intValue();
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(fieldDescriptor, convertFrom(row.get(fieldDescriptor.getName())));
    }
}
