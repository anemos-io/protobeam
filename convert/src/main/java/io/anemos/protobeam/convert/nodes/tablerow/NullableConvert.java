package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class NullableConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

    private AbstractConvert field;
    private Descriptors.FieldDescriptor valueFieldDescriptor;

    public NullableConvert(Descriptors.FieldDescriptor fieldDescriptor, AbstractConvert field) {
        super(fieldDescriptor);
        this.field = field;
        this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");

    }

    @Override
    public Object convert(Object in) {
        return in;
    }

    @Override
    public void convert(Message message, TableRow row) {
        Message nullableWrapperMessage = (Message) message.getField(fieldDescriptor);
        if (!valueFieldDescriptor.getDefaultValue().equals(nullableWrapperMessage.getField(valueFieldDescriptor))) {
            Object value = convert(nullableWrapperMessage.getField(valueFieldDescriptor));
            row.set(fieldDescriptor.getName(), value);
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        if (row.containsKey(fieldDescriptor.getName())) {
            Object obj = row.get(fieldDescriptor.getName());
            DynamicMessage wrapperMessage = DynamicMessage.newBuilder(fieldDescriptor.getMessageType()).setField(valueFieldDescriptor, obj).build();
            builder.setField(fieldDescriptor, wrapperMessage);
        }
    }

}
