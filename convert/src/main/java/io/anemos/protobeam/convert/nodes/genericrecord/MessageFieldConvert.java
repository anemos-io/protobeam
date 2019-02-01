package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

class MessageFieldConvert extends AbstractGenericRecordConvert<Object> {
    AbstractMessageConvert convert;

    public MessageFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractMessageConvert convert) {
        super(descriptor);
        this.convert = convert;
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        GenericData.Record nested = (GenericData.Record) row.get(descriptor.getName());
        if (nested != null) {
            DynamicMessage.Builder fieldBuilder = DynamicMessage.newBuilder(descriptor.getMessageType());
            convert.convertToProto(fieldBuilder, nested);
            builder.setField(descriptor, fieldBuilder.build());
        }
    }
}
