package io.anemos.protobeam.transform.beamsql;

import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

public class BeamSqlPipelineTest {

    @Test
    public void protoSum() {
        PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<ProtoBeamBasicPrimitive> inProto =
                PBegin.in(pipeline)
                        .apply(Create.of(
                                ProtoBeamBasicPrimitive.newBuilder()
                                        .setPrimitiveInt32(1)
                                        .build(),
                                ProtoBeamBasicPrimitive.newBuilder()
                                        .setPrimitiveInt32(2)
                                        .build()
                                , ProtoBeamBasicPrimitive.newBuilder()
                                        .setPrimitiveInt32(3)
                                        .build()
                        ));

        PCollection<Row> rows = inProto.apply(ParDo.of(new ProtoToRowDoFn<>(ProtoBeamBasicPrimitive.class)))
                .setRowSchema(new SchemaProtoToBeamSQL().getSchema(ProtoBeamBasicPrimitive.getDescriptor()));

        PCollection<Row> result = rows.apply(SqlTransform.query("select SUM(primitive_int32) from PCOLLECTION"));

        result.apply(ParDo.of(new OutputFn()));
        pipeline.run().waitUntilFinish();
    }

}
