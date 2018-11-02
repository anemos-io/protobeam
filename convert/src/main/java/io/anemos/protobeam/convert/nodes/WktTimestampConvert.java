package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.generic.GenericRecord;

import java.text.ParseException;

public class WktTimestampConvert extends AbstractConvert {
    public WktTimestampConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
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
    public void convertToProto(Message.Builder message, TableRow row) {
        String cell = (String) row.get(descriptor.getName());
        if(cell != null) {
            try {
                message.setField(descriptor, Timestamps.parse(cell));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {

    }

    public static boolean isHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        return ".google.protobuf.Timestamp".equals(fieldDescriptor.toProto().getTypeName());
    }

}
