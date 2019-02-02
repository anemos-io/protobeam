package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.avro.generic.GenericRecord;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;

class NullableConvert extends AbstractGenericRecordConvert<Object> {

    private AbstractConvert field;
    private Descriptors.FieldDescriptor valueFieldDescriptor;

    public NullableConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
        this.valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        if (null != row.get(fieldDescriptor.getName())) {
            Object obj = row.get(fieldDescriptor.getName());
            DynamicMessage wrapperMessage = DynamicMessage.newBuilder(fieldDescriptor.getMessageType()).setField(valueFieldDescriptor, obj).build();
            builder.setField(fieldDescriptor, wrapperMessage);
        }
    }

}
