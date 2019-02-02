package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

public class BooleanFieldConvert extends AbstractBeamSqlConvert<Boolean> {
    public BooleanFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        builder.setField(fieldDescriptor, convertFrom(row.getValue(fieldDescriptor.getName())));
    }
}
