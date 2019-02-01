package io.anemos.protobeam.transform.beamsql;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class ProtoToRowDoFn<T extends Message> extends DoFn<T, Row> {

    private ProtoBeamSqlExecutionPlan plan;

    public ProtoToRowDoFn(Class<T> messageClass) {
        plan = new ProtoBeamSqlExecutionPlan(messageClass);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(plan.convert(c.element()));
    }
}
