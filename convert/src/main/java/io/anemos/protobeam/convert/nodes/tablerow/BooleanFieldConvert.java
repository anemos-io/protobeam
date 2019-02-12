package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class BooleanFieldConvert extends AbstractConvert<Boolean, TableRow, Map<String, Object>> {
    public BooleanFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, TableRow row) {
        row.set(fieldDescriptor.getName(), fromProtoValue(message.getField(fieldDescriptor)));
    }

    @Override
    public void toProto(Map<String, Object> row, Message.Builder builder) {
        builder.setField(fieldDescriptor, toProtoValue(row.get(fieldDescriptor.getName())));
    }

    @Override
    public Boolean toProtoValue(Object in) {
        return (Boolean) in;
    }
}
