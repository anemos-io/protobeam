package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.tablerow.BigQueryTableRowNodeFactory;

import java.io.Serializable;

public class ProtoTableRowExecutionPlan<T extends Message> extends AbstractExecutionPlan<T> implements Serializable {

    public ProtoTableRowExecutionPlan(Message message) {
        this((Class<T>) message.getClass());
    }

    public ProtoTableRowExecutionPlan(Class<T> messageClass) {
        super(messageClass, new BigQueryTableRowNodeFactory());
    }

    public TableRow convert(T message) {
        TableRow row = new TableRow();
        convert.convert(message, row);
        return row;
    }

    public T convertToProto(TableRow row) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        convert.convertToProto(builder, row);
        return convertToConcrete(builder);
    }
}
