package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class NullableConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

    private AbstractConvert field;

    public NullableConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public Object convert(Object in) {
        return in;
    }

    @Override
    public void convert(Message message, TableRow row) {
        if (message.hasField(fieldDescriptor)) {
            field.convert(message, row);
        }
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        Object cell = row.get(fieldDescriptor.getName());
        if (cell != null) {
            field.convertToProto(builder, field);
        }
    }

}
