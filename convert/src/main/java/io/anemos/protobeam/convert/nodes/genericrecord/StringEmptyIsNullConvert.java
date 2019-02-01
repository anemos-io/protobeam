package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class StringEmptyIsNullConvert extends AbstractGenericRecordConvert<Object> {
    public StringEmptyIsNullConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {

    }
}
