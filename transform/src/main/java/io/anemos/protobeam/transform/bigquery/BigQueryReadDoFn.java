package io.anemos.protobeam.transform.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoTableRowExecutionPlan;
import org.apache.beam.sdk.transforms.DoFn;

public class BigQueryReadDoFn<T extends Message> extends DoFn<TableRow, T> {

    private ProtoTableRowExecutionPlan plan;

    public BigQueryReadDoFn(Class<T> messageClass) {
        plan = new ProtoTableRowExecutionPlan(messageClass);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output((T) plan.convertToProto(c.element()));
    }
}
