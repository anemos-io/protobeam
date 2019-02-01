package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.util.List;
import java.util.Map;

class MessageConvert extends AbstractMessageConvert<Object, TableRow, Map<String, Object>> {

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
    public void convert(Message message, TableRow row) {
        fields.forEach(
                e -> e.convert(message, row)
        );
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        fields.forEach(e -> e.convertToProto(builder, row));
    }
}
