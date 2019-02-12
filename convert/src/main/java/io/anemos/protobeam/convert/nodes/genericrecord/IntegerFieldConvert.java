package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class IntegerFieldConvert extends AbstractGenericRecordConvert<Object> {
    public IntegerFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object toProtoValue(Object in) {
        return ((Long) in).intValue();
    }

    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        builder.setField(fieldDescriptor, toProtoValue(row.get(fieldDescriptor.getName())));
    }
}
