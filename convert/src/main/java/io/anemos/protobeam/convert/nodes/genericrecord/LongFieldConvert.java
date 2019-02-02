package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class LongFieldConvert extends AbstractGenericRecordConvert<Object> {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        Long value = (Long) row.get(fieldDescriptor.getName());
        builder.setField(fieldDescriptor, convertFrom(value));
    }
}
