package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class LongFieldConvert extends AbstractGenericRecordConvert<Object> {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        Long value = (Long) row.get(fieldDescriptor.getName());
        builder.setField(fieldDescriptor, toProtoValue(value));
    }
}
