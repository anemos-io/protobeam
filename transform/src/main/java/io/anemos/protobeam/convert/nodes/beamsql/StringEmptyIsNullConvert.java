package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.values.Row;

class StringEmptyIsNullConvert extends AbstractBeamSqlConvert<Object> {
    public StringEmptyIsNullConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void toProto(Row row, Message.Builder builder) {

    }
}
