package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.util.Map;

public class FlattenFieldConvert extends AbstractMessageConvert<Object, TableRow, Map<String, Object>> {

    AbstractMessageConvert convert;

    public FlattenFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }


    @Override
    public void fromProto(Message message, TableRow row) {
        TableRow tmp = new TableRow();

        Message field = (Message) message.getField(fieldDescriptor);
        convert.fromProto(field, tmp);

        for (String key : tmp.keySet()) {
            row.set(key, tmp.get(key));
        }
    }

    @Override
    public void toProto(Map row, Message.Builder builder) {
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        fieldDescriptor.getMessageType().getFields().forEach(subFieldDescriptor -> {
            Object value = row.get(subFieldDescriptor.getName());
            dynamicMessageBuilder.setField(subFieldDescriptor, value);
        });
        builder.setField(fieldDescriptor, dynamicMessageBuilder.build());
    }
}
