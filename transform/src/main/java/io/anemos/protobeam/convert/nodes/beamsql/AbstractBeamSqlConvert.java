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
    public Object convert(Object in) {
        return in;
    }

    @Override
    public void convert(Message message, Row.Builder row) {
        row.addValue(convert(message.getField(fieldDescriptor)));
    }
}
