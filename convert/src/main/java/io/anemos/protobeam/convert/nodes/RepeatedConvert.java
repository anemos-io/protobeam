package io.anemos.protobeam.convert.nodes;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

public class RepeatedConvert extends AbstractConvert {

    private AbstractConvert field;

    public RepeatedConvert(Descriptors.FieldDescriptor descriptor, AbstractConvert field) {
        super(descriptor);
        this.field = field;
    }

    @Override
    public void convert(Message message, TableRow row) {
        List tableCells = new ArrayList<>();
        ((List) message.getField(descriptor)).forEach(
                obj -> tableCells.add(field.convert(obj))
        );
        row.set(descriptor.getName(), tableCells);
    }

    @Override
    public void convertToProto(Message.Builder builder, TableRow row) {
        List list = (List) row.get(descriptor.getName());
        list.forEach(
                obj -> builder.addRepeatedField(descriptor, obj)
        );
    }

    @Override
    public void convertToProto(Message.Builder builder, GenericRecord row) {
        List list = (List) row.get(descriptor.getName());
        list.forEach(
                obj -> builder.addRepeatedField(descriptor, field.convertFromGenericRecord(obj))
        );
    }
}
