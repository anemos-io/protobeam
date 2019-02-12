package io.anemos.protobeam.convert.nodes.genericrecord;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

class MessageConvert extends AbstractMessageConvert<Object, GenericRecord, GenericRecord> {

    private List<AbstractConvert> fields;

    public MessageConvert(Descriptors.FieldDescriptor descriptor, List<AbstractConvert> fields) {
        super(descriptor);

        this.fields = fields;
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, GenericRecord row) {
        fields.forEach(
                e -> e.fromProto(message, row)
        );
    }

    @Override
    public void toProto(GenericRecord row, Message.Builder builder) {
        fields.forEach(e -> e.toProto(row, builder));
    }


}
