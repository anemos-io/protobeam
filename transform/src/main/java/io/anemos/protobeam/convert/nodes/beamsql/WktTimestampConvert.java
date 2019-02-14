package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableDateTime;

class WktTimestampConvert extends AbstractBeamSqlConvert<Object> {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object toProtoValue(Object in) {
        return Timestamps.fromMicros(((ReadableDateTime) in).getMillis());
    }

    @Override
    public void toProto(Row row, Message.Builder builder) {
        ReadableDateTime column = row.getDateTime(fieldDescriptor.getName());
        if (column != null) {
            builder.setField(fieldDescriptor, toProtoValue(row.getDateTime(fieldDescriptor.getName())));
        }
    }

    @Override
    public void fromProto(Message message, Row.Builder row) {
        if (message.hasField(fieldDescriptor)) {
            Timestamp timestamp = (Timestamp) message.getField(fieldDescriptor);
            row.addValue(Timestamps.toMicros(timestamp));
        } else {
            row.addValue(null);
        }
    }
}
