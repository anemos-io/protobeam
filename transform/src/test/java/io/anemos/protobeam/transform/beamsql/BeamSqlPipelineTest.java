package io.anemos.protobeam.transform.beamsql;

import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BeamSqlPipelineTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void initPipeline() {
    ProtoBeamSqlSchema.registerIn(pipeline).message(ProtoBeamBasicPrimitive.class);
  }

  private void assertContains(PCollection<Row> result, String... elements) {
    PCollection<String> test = result.apply(ParDo.of(new ToStringFn()));
    PAssert.that(test).containsInAnyOrder(elements);
  }

  @Test
  public void protoSum() {
    PCollection<Row> result =
        PBegin.in(pipeline)
            .apply(
                Create.of(
                    ProtoBeamBasicPrimitive.newBuilder().setPrimitiveInt32(1).build(),
                    ProtoBeamBasicPrimitive.newBuilder().setPrimitiveInt32(2).build(),
                    ProtoBeamBasicPrimitive.newBuilder().setPrimitiveInt32(3).build()))
            .apply(SqlTransform.query("select SUM(primitive_int32) from PCOLLECTION"));

    assertContains(result, "Row:[6]");
    pipeline.run();
  }
}
