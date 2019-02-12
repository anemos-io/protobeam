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
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, Row.Builder row) {
        fields.forEach(
                e -> e.fromProto(message, row)
        );
    }

    @Override
    public void toProto(Row row, Message.Builder builder) {
        fields.forEach(e -> e.toProto(row, builder));
    }


}
