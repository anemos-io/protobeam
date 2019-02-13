package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class NullableFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

    private AbstractConvert field;

    public NullableFieldConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, TableRow row) {
        if (message.hasField(fieldDescriptor)) {
            field.fromProto(message, row);
        }
    }

    @Override
    public void toProto(Map row, Message.Builder builder) {
        Object cell = row.get(fieldDescriptor.getName());
        if (cell != null) {
            field.toProto(field, builder);
        }
    }

}
