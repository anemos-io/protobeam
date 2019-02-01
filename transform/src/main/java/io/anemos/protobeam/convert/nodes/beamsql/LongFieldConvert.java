package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class LongFieldConvert extends AbstractBeamSqlConvert<Object> {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        Long value = row.getInt64(descriptor.getName());
        builder.setField(descriptor, convertFrom(value));
    }
}
