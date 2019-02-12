package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

class StringEmptyIsNullConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {
    public StringEmptyIsNullConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void fromProto(Message message, TableRow row) {
        String fieldName = fieldDescriptor.getName();
//        if (fieldDescriptor.isRepeated()) {
//            List<Object> tableCells = new ArrayList<>();
//            for (String value : (List<String>) message.getField(fieldDescriptor))
//                if (!value.isEmpty())
//                    tableCells.add(value);
//            row.set(fieldName, tableCells);
//        } else {
        String value = String.valueOf(message.getField(fieldDescriptor));
        if (!value.isEmpty()) {
            row.set(fieldName, value);
        }
//        }

    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void toProto(Map row, Message.Builder message) {

    }
}
