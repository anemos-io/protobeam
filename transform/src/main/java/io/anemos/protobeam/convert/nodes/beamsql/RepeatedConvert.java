package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

class RepeatedConvert extends AbstractBeamSqlConvert<Object> {

    private AbstractConvert field;

    public RepeatedConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public void convert(Message message, Row.Builder row) {
        List list = new ArrayList<>();
        ((List) message.getField(fieldDescriptor)).forEach(
                obj -> list.add(field.convert(obj))
        );
        row.addArray(list);
    }


    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        List list = row.getArray(fieldDescriptor.getName());
        list.forEach(
                obj -> builder.addRepeatedField(fieldDescriptor, field.convertFrom(obj))
        );
    }
}
