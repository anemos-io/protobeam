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
    public Object convert(Object in) {
        return in;
    }

    @Override
    public void convert(Message message, GenericRecord row) {
        fields.forEach(
                e -> e.convert(message, row)
        );
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        fields.forEach(e -> e.convertToProto(builder, row));
    }


}
