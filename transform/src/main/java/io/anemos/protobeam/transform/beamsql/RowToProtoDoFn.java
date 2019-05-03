package io.anemos.protobeam.transform.beamsql;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class RowToProtoDoFn<T extends Message> extends DoFn<Row, T> {

  private ProtoBeamSqlExecutionPlan plan;

  public RowToProtoDoFn(Class<T> messageClass) {
    plan = new ProtoBeamSqlExecutionPlan(messageClass);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    c.output((T) plan.convertToProto(c.element()));
  }
}
