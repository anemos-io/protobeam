package io.anemos.protobeam.convert;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.nodes.genericrecord.BigQueryGenericRecordNodeFactory;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public class ProtoGenericRecordExecutionPlan<T extends Message> extends AbstractExecutionPlan<T> implements Serializable {

    public ProtoGenericRecordExecutionPlan(Message message) {
        this((Class<T>) message.getClass());
    }

    public ProtoGenericRecordExecutionPlan(Class<T> messageClass) {
        super(messageClass, new BigQueryGenericRecordNodeFactory());
    }

    public GenericRecord convert(T message) {
        throw new RuntimeException();
    }

    public T convertToProto(GenericRecord row) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        convert.toProto(row, builder);
        return convertToConcrete(builder);
    }
}
