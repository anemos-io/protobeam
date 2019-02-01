package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class StringFieldConvert extends AbstractBeamSqlConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public String convertFrom(Object in) {
        return in.toString();
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        builder.setField(descriptor, row.getValue(descriptor.getName()).toString());
    }
}
