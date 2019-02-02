package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

class NullableConvert extends AbstractBeamSqlConvert<Object> {

    private AbstractConvert field;
    private Descriptors.FieldDescriptor valueFieldDescriptor;


    public NullableConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
        this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
    }

    @Override
    public void convert(Message message, Row.Builder row) {
        Message nullableWrapperMessage = (Message) message.getField(fieldDescriptor);
        if (!valueFieldDescriptor.getDefaultValue().equals(nullableWrapperMessage.getField(valueFieldDescriptor))) {
            Object value = convert(nullableWrapperMessage.getField(valueFieldDescriptor));
            row.addValue(value);
        } else {
            row.addValue(null);
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        Object value = row.getValue(fieldDescriptor.getName());
        if (null != value) {
            DynamicMessage wrapperMessage = DynamicMessage.newBuilder(fieldDescriptor.getMessageType()).setField(valueFieldDescriptor, value).build();
            builder.setField(fieldDescriptor, wrapperMessage);
        }
    }
}
