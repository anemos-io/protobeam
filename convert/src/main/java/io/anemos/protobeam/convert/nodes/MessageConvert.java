package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;

public class MessageConvert extends AbstractConvert {

    private List<AbstractConvert> fields;

    public MessageConvert(Descriptors.FieldDescriptor descriptor, List<AbstractConvert> fields) {
        super(descriptor);

        this.fields = fields;
    }

    @Override
    public void convert(Message message, TableRow row) {

        fields.forEach(
                e -> e.convert(message, row)
        );
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        fields.forEach(e -> e.convertToProto(builder, row));
    }


}
