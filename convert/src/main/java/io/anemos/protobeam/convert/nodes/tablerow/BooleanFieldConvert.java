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
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public void convertToProto(Message.Builder builder, Map<String, Object> row) {
        builder.setField(descriptor, convertFrom(row.get(descriptor.getName())));
    }

    @Override
    public Boolean convertFrom(Object in) {
        return (Boolean) in;
    }
}
