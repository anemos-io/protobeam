package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.util.Map;

public class FlattenConvert extends AbstractMessageConvert<Object, TableRow, Map<String, Object>> {

    AbstractMessageConvert convert;

    public FlattenConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public Object convert(Object in) {
        return in;
    }


    @Override
    public void convert(Message message, TableRow row) {
        Message field = (Message) message.getField(fieldDescriptor);
        fieldDescriptor.getMessageType().getFields().forEach( subfieldDescriptor -> {
            Object value = convert.convert(field.getField(subfieldDescriptor));
            row.set(subfieldDescriptor.getName(), value);
        });
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        fieldDescriptor.getMessageType().getFields().forEach(subFieldDescriptor -> {
            Object value = row.get(subFieldDescriptor.getName());
            dynamicMessageBuilder.setField(subFieldDescriptor, value);
        });
        builder.setField(fieldDescriptor, dynamicMessageBuilder.build());
    }
}
