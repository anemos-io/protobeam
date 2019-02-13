package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.util.TimestampUtil;

import java.util.Map;

class WktTimestampFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
    public WktTimestampFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void fromProto(Message message, TableRow row) {
        if (message.hasField(fieldDescriptor)) {
            Timestamp timestamp = (Timestamp) message.getField(fieldDescriptor);
            row.set(fieldDescriptor.getName(), Timestamps.toString(timestamp));
        }
    }

    @Override
    public Object toProtoValue(Object in) {
        return TimestampUtil.fromBQ((String) in);
    }

    @Override
    public void toProto(Map<String, Object> row, Message.Builder message) {
        String cell = (String) row.get(fieldDescriptor.getName());
        if (cell != null) {
            message.setField(fieldDescriptor, toProtoValue(cell));
        }
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in.toString();
    }
}
