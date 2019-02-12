package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.beam.sdk.values.Row;

public abstract class AbstractBeamSqlConvert<T> extends AbstractConvert<T, Row.Builder, Row> {
    public AbstractBeamSqlConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, Row.Builder row) {
        row.addValue(fromProtoValue(message.getField(fieldDescriptor)));
    }
}
