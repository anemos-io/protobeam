package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

class WktWrapperConvert extends AbstractGenericRecordConvert<Object> {

    private Descriptors.FieldDescriptor valueFieldDescriptor;

    public WktWrapperConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
        this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
    }

    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        if (null != row.get(fieldDescriptor.getName())) {
            Object obj = row.get(fieldDescriptor.getName());
            DynamicMessage wrapperMessage = DynamicMessage.newBuilder(fieldDescriptor.getMessageType()).setField(valueFieldDescriptor, obj).build();
            builder.setField(fieldDescriptor, wrapperMessage);
        }
    }

}
