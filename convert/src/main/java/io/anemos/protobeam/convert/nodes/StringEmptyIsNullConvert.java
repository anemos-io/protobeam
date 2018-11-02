package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

public class StringEmptyIsNullConvert extends AbstractConvert {
    public StringEmptyIsNullConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void convert(Message message, TableRow row) {
        String fieldName = descriptor.getName();
//        if (fieldDescriptor.isRepeated()) {
//            List<Object> tableCells = new ArrayList<>();
//            for (String value : (List<String>) message.getField(fieldDescriptor))
//                if (!value.isEmpty())
//                    tableCells.add(value);
//            row.set(fieldName, tableCells);
//        } else {
        String value = String.valueOf(message.getField(descriptor));
        if (!value.isEmpty()) {
            row.set(fieldName, value);
        }
//        }

    }

    @Override
    public void convertToProto(Message.Builder message, TableRow row) {

    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {

    }
}
