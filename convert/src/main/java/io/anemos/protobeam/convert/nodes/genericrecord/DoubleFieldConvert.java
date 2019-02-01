package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class DoubleFieldConvert extends AbstractGenericRecordConvert<Object> {
    public DoubleFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        Double value = (Double) row.get(descriptor.getName());
        builder.setField(descriptor, convertFrom(value));
    }
}
