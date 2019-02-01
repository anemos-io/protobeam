package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class StringFieldConvert extends AbstractGenericRecordConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public String convertFrom(Object in) {
        return in.toString();
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, row.get(descriptor.getName()).toString());
    }
}
