package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class DoubleFieldConvert extends AbstractBeamSqlConvert<Object> {
    public DoubleFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void toProto(Row row, Message.Builder builder) {
        Double value = row.getDouble(fieldDescriptor.getName());
        builder.setField(fieldDescriptor, toProtoValue(value));
    }
}
