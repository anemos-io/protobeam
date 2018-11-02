package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

public class LongFieldConvert extends AbstractConvert {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), message.getField(descriptor));
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        builder.setField(descriptor, row.get(descriptor.getName()));
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        Long value = (Long) row.get(descriptor.getName());
        builder.setField(descriptor, convertFromGenericRecord(value));
    }
}
