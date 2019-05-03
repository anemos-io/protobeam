package io.anemos.protobeam.transform.bigquery;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoGenericRecordExecutionPlan;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class BigQueryReadFormatFunction<T extends Message>
    implements SerializableFunction<SchemaAndRecord, T> {

  private ProtoGenericRecordExecutionPlan plan;

  public BigQueryReadFormatFunction(Class<T> messageClass) {
    plan = new ProtoGenericRecordExecutionPlan(messageClass);
  }

  @Override
  public T apply(SchemaAndRecord input) {
    return (T) plan.convertToProto(input.getRecord());
  }
}
