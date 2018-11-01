package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

public class StringFieldConvert extends AbstractConvert<String> {
    public StringFieldConvert(Descriptors.FieldDescriptor descriptor) {
        super(descriptor);
    }


    @Override
    public String convert(Object in) {
        return String.valueOf(in);
    }

    @Override
    public void convert(Message message, TableRow row) {
        String fieldName = descriptor.getName();
        String value = convert(message.getField(descriptor));
        row.set(fieldName, value);
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        builder.setField(descriptor, row.get(descriptor.getName()));
    }
}
