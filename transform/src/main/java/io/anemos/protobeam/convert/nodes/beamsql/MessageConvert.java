package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public class MessageConvert extends AbstractMessageConvert<Object, Row.Builder, Row> {

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
    public void convert(Message message, Row.Builder row) {
        fields.forEach(
                e -> e.convert(message, row)
        );
    }

    @Override
    public void convertToProto(Message.Builder builder, Row row) {
        fields.forEach(e -> e.convertToProto(builder, row));
    }


}
