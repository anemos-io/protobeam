package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.generic.GenericRecord;

class WktTimestampConvert extends AbstractGenericRecordConvert<Object> {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convertFrom(Object in) {
        return Timestamps.fromMicros((Long) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(fieldDescriptor, convertFrom(row.get(fieldDescriptor.getName())));
    }

}
