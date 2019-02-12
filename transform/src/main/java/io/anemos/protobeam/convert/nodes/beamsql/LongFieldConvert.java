package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class LongFieldConvert extends AbstractBeamSqlConvert<Object> {
    public LongFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public void toProto(Row row, Message.Builder builder) {
        Long value = row.getInt64(fieldDescriptor.getName());
        builder.setField(fieldDescriptor, toProtoValue(value));
    }
}
