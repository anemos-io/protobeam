package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.util.TimestampUtil;

import java.util.Map;

class WktTimestampConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        if (message.hasField(fieldDescriptor)) {
            Timestamp timestamp = (Timestamp) message.getField(fieldDescriptor);
            // TODO: WHY check on default instance?
            row.set(fieldDescriptor.getName(), Timestamps.toString(timestamp));
        }
    }

    @Override
    public Object convertFrom(Object in) {
        return TimestampUtil.fromBQ((String) in);
    }

    @Override
    public void convertToProto(Message.Builder message, Map<String, Object> row) {
        String cell = (String) row.get(fieldDescriptor.getName());
        if (cell != null) {
            message.setField(fieldDescriptor, convertFrom(cell));
        }
    }

    @Override
    public Object convert(Object in) {
        return in.toString();
    }
}
