package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

class RepeatedConvert extends AbstractGenericRecordConvert<Object> {

    private AbstractConvert field;

    public RepeatedConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        List list = (List) row.get(fieldDescriptor.getName());
        list.forEach(
                obj -> builder.addRepeatedField(fieldDescriptor, field.convertFrom(obj))
        );
    }
}
