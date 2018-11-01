package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

public class MessageFieldConvert extends AbstractConvert {
    MessageConvert convert;

    public MessageFieldConvert(Descriptors.FieldDescriptor descriptor, MessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public void convert(Message message, TableRow row) {
        if (message.hasField(descriptor)) {
            TableRow nested = new TableRow();
            convert.convert((AbstractMessage) message.getField(descriptor), nested);
            row.set(descriptor.getName(), nested);
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        //Message.Builder fieldBuilder = builder.getFieldBuilder(descriptor);
        TableRow nested = (TableRow) row.get(descriptor.getName());
        if (nested != null) {
            DynamicMessage.Builder fieldBuilder = DynamicMessage.newBuilder(descriptor.getMessageType());
            convert.convertToProto(fieldBuilder, nested);
            builder.setField(descriptor, fieldBuilder.build());
        }
    }
}
