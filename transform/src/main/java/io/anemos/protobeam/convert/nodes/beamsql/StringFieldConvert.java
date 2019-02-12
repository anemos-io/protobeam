package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class StringFieldConvert extends AbstractBeamSqlConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public String toProtoValue(Object in) {
        return in.toString();
    }

    @Override
    public void toProto(Row row, Message.Builder builder) {
        builder.setField(fieldDescriptor, row.getValue(fieldDescriptor.getName()).toString());
    }
}
