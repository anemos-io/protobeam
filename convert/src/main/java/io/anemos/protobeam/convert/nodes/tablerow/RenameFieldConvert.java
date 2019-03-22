package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.AbstractConvert;

import java.util.Map;

public class RenameFieldConvert extends AbstractConvert<Object, TableRow, Map<String, Object>> {

    AbstractConvert convert;
    Descriptors.FieldDescriptor descriptor;
    String renameTo;

    public RenameFieldConvert(String renameTo, Descriptors.FieldDescriptor descriptor, AbstractConvert convert) {
        super(descriptor);
        this.convert = convert;
        this.descriptor = descriptor;
        this.renameTo = renameTo;
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }


    @Override
    public void fromProto(Message message, TableRow row) {
        TableRow tmp = new TableRow();
        convert.fromProto(message, tmp);
        Object value = tmp.get(descriptor.getName());
        row.set(renameTo, value);
    }

    @Override
    public void toProto(Map row, Message.Builder builder) {
        TableRow tmp = new TableRow();
        Object value = row.get(renameTo);
        tmp.set(descriptor.getName(), value);
        convert.toProto(tmp, builder);
    }
}
