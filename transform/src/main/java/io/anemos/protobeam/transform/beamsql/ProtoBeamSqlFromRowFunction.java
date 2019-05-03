package io.anemos.protobeam.transform.beamsql;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

public class ProtoBeamSqlFromRowFunction<T extends Message>
    implements SerializableFunction<Row, T> {

  private ProtoBeamSqlExecutionPlan plan;

  public ProtoBeamSqlFromRowFunction(Class<T> messageClass) {
    plan = new ProtoBeamSqlExecutionPlan(messageClass);
  }

  @Override
  public T apply(Row input) {
    return (T) plan.convertToProto(input);
  }
}
