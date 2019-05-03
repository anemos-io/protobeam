package io.anemos.protobeam.transform.beamsql;

import com.google.protobuf.Message;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.SchemaRegistry;

public class ProtoBeamSqlSchema {

  private SchemaRegistry schemaRegistry;

  private ProtoBeamSqlSchema(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public static ProtoBeamSqlSchema registerIn(Pipeline pipeline) {
    return new ProtoBeamSqlSchema(pipeline.getSchemaRegistry());
  }

  public <T extends Message> ProtoBeamSqlSchema message(Class<T> messageClass) {
    schemaRegistry.registerSchemaForClass(
        messageClass,
        new SchemaProtoToBeamSQL().getSchema(messageClass),
        new ProtoBeamSqlToRowFunction<>(messageClass),
        new ProtoBeamSqlFromRowFunction<>(messageClass));

    return this;
  }
}
