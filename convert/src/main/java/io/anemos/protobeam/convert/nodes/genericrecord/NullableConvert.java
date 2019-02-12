package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import org.apache.avro.generic.GenericRecord;

class NullableConvert extends AbstractGenericRecordConvert<Object> {

    private AbstractConvert field;

    public NullableConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        Object cell = row.get(fieldDescriptor.getName());
        if (cell != null) {
            field.toProto(row, builder);
        }
    }
}
