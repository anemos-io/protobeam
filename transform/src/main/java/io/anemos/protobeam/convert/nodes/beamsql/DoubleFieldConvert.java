package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class DoubleFieldConvert extends AbstractBeamSqlConvert<Object> {
    public DoubleFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        Double value = row.getDouble(fieldDescriptor.getName());
        builder.setField(fieldDescriptor, convertFrom(value));
    }
}
