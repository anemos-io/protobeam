package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class FloatFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
    public FloatFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        row.set(descriptor.getName(), convert(message.getField(descriptor)));
    }

    @Override
    public Object convert(Object in) {
        return ((Float) in).doubleValue();
    }

    @Override
    public Object convertFrom(Object in) {
        return ((Double) in).floatValue();
    }

    @Override
    public void convertToProto(Message.Builder builder, Map row) {
        builder.setField(descriptor, convertFrom(row.get(descriptor.getName())));
    }
}
