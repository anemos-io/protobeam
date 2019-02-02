package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.values.Row;

class WktTimestampConvert extends AbstractBeamSqlConvert<Object> {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object convertFrom(Object in) {
        return Timestamps.fromMicros((Long) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        builder.setField(fieldDescriptor, convertFrom(row.getDateTime(fieldDescriptor.getName())));
    }

}
