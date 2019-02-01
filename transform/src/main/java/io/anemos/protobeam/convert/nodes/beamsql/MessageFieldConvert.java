package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.beam.sdk.values.Row;

public class MessageFieldConvert extends AbstractBeamSqlConvert<Object> {
    AbstractMessageConvert convert;

    public MessageFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        Row nested = row.getRow(descriptor.getName());
        if (nested != null) {
            DynamicMessage.Builder fieldBuilder = DynamicMessage.newBuilder(descriptor.getMessageType());
            convert.convertToProto(fieldBuilder, nested);
            builder.setField(descriptor, fieldBuilder.build());
        }
    }
}
