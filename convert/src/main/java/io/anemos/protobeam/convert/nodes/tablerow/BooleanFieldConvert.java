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
    public Object convert(Object in) {
        return in;
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(fieldDescriptor.getName(), convert(message.getField(fieldDescriptor)));
    }

    @Override
    public void convertToProto(Message.Builder builder, Map<String, Object> row) {
        builder.setField(fieldDescriptor, convertFrom(row.get(fieldDescriptor.getName())));
    }

    @Override
    public Boolean convertFrom(Object in) {
        return (Boolean) in;
    }
}
