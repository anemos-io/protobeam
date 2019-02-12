package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class StringFieldConvert extends AbstractGenericRecordConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public String toProtoValue(Object in) {
        return in.toString();
    }

    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        builder.setField(fieldDescriptor, row.get(fieldDescriptor.getName()).toString());
    }
}
