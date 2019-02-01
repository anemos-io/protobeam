package io.anemos.protobeam.transform.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoTableRowExecutionPlan;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class BigQueryWriteFormatFunction<T extends Message> implements SerializableFunction<T, TableRow> {

    private ProtoTableRowExecutionPlan plan;

    public BigQueryWriteFormatFunction(Class<T> messageClass) {
        plan = new ProtoTableRowExecutionPlan(messageClass);
    }

    @Override
    public TableRow apply(T input) {

        TableRow row = plan.convert(input);
        return row ;
    }
}
