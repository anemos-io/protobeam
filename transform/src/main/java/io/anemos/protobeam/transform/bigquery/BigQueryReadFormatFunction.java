package io.anemos.protobeam.transform.bigquery;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBigQueryExecutionPlan;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class BigQueryReadFormatFunction<T extends Message> implements SerializableFunction<SchemaAndRecord, T> {

    private ProtoBigQueryExecutionPlan plan;

    public BigQueryReadFormatFunction(Class<T> messageClass) {
        plan = new ProtoBigQueryExecutionPlan(messageClass);
    }

    @Override
    public T apply(SchemaAndRecord input) {
        return (T) plan.convertToProto(input.getRecord());
    }
}
