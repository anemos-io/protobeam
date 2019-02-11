package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.util.Map;

class MessageFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
    AbstractMessageConvert convert;

    public MessageFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public Object convert(Object in) {
        TableRow nested = new TableRow();
        convert.convert((Message) in, nested);
        return nested;
    }

    @Override
    public void convert(Message message, TableRow row) {
        if (message.hasField(fieldDescriptor)) {
            TableRow nested = new TableRow();
            convert.convert((AbstractMessage) message.getField(fieldDescriptor), nested);
            row.set(fieldDescriptor.getName(), nested);
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        Map nested = (Map) row.get(fieldDescriptor.getName());
        if (nested != null) {
            DynamicMessage.Builder fieldBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
            convert.convertToProto(fieldBuilder, nested);
            builder.setField(fieldDescriptor, fieldBuilder.build());
        }
    }

    @Override
    public Object convertFrom(Object in) {
        DynamicMessage.Builder fieldBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        convert.convertToProto(fieldBuilder, in);
        return fieldBuilder.build();
    }
}
