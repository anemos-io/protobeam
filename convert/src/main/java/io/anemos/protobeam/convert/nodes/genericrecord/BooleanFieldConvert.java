package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class BooleanFieldConvert extends AbstractGenericRecordConvert<Boolean> {
    public BooleanFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, convertFrom(row.get(descriptor.getName())));
    }
}
