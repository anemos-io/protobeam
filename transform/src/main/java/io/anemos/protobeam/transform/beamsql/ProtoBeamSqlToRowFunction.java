package io.anemos.protobeam.transform.beamsql;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

public class ProtoBeamSqlToRowFunction<T extends Message> implements SerializableFunction<T, Row> {

    private ProtoBeamSqlExecutionPlan plan;

    public ProtoBeamSqlToRowFunction(Class<T> messageClass) {
        plan = new ProtoBeamSqlExecutionPlan(messageClass);
    }

    @Override
    public Row apply(T input) {
        return plan.convert(input);
    }
}
