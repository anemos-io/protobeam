package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class FloatFieldConvert extends AbstractGenericRecordConvert<Object> {
    public FloatFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convertFrom(Object in) {
        return ((Double) in).floatValue();
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, convertFrom(row.get(descriptor.getName())));
    }
}
