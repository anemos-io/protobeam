package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.util.TimestampUtil;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class WktTimestampConvert extends AbstractConvert {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    public static boolean isHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        return ".google.protobuf.Timestamp".equals(fieldDescriptor.toProto().getTypeName());
    }

    @Override
    public void convert(Message message, TableRow row) {
        if (message.hasField(descriptor)) {
            Timestamp timestamp = (Timestamp) message.getField(descriptor);
            // TODO: WHY check on default instance?
            row.set(descriptor.getName(), Timestamps.toString(timestamp));
        }
    }

    @Override
    public Object convertFromTableCell(Object in) {
        return TimestampUtil.fromBQ((String) in);
    }

    @Override
    public void convertToProto(Message.Builder message, Map row) {
        String cell = (String) row.get(descriptor.getName());
        if (cell != null) {
            message.setField(descriptor, convertFromTableCell(cell));
        }
    }

    @Override
    public Object convert(Object in) {
        return in.toString();
    }

    @Override
    public Object convertFromGenericRecord(Object in) {
        return Timestamps.fromMicros((Long) in);
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        builder.setField(descriptor, convertFromGenericRecord(row.get(descriptor.getName())));
    }

}
