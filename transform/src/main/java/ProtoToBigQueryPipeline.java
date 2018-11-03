import com.google.protobuf.ByteString;
import io.anemos.protobeam.examples.*;
import io.anemos.protobeam.test.BytesGenerator;
import io.anemos.protobeam.test.TimestampGenerator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;

public class ProtoToBigQueryPipeline extends DemoPipelineBase {

    private transient BytesGenerator bytesGenerator = new BytesGenerator();
    private transient TimestampGenerator tsGenerator = new TimestampGenerator();

    public static void main(String... args) {
        new ProtoToBigQueryPipeline().run();
    }

    private void run() {
        Pipeline pipeline = createPipeline();
        toBqrimitive(pipeline);
        toBqepeat(pipeline);
        toBqMessage(pipeline);
        toBqOption(pipeline);
        toBqWkt(pipeline);
        pipeline.run();
    }

    private void toBqrimitive(Pipeline pipeline) {
        ProtoBeamBasicPrimitive m1 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(1)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveString("abc")
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        ProtoBeamBasicPrimitive m2 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(2)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveDouble(42.13)
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        ProtoBeamBasicPrimitive m3 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(3)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveInt32(14)
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicPrimitive.class, ProtoBeamBasicPrimitive.getDescriptor(), "protobeam_primitive"));
    }

    private void toBqepeat(Pipeline pipeline) {
        ProtoBeamBasicRepeatPrimitive m1 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(1)
                .setTestName("ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedDouble(12.34)
                .addRepeatedDouble(56.78)
                .build();
        ProtoBeamBasicRepeatPrimitive m2 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(2)
                .setTestName("ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedInt32(12)
                .addRepeatedInt32(34)
                .build();
        ProtoBeamBasicRepeatPrimitive m3 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(3)
                .setTestName("ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedString("foo")
                .addRepeatedString("bar")
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicRepeatPrimitive.class, ProtoBeamBasicRepeatPrimitive.getDescriptor(), "protobeam_repeat"));
    }

    private void toBqMessage(Pipeline pipeline) {
        ProtoBeamBasicMessage m1 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(1)
                .setTestName("ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveString("abc")
                        .build()
                ).build();
        ProtoBeamBasicMessage m2 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(2)
                .setTestName("ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveDouble(42.13)
                        .build()
                ).build();
        ProtoBeamBasicMessage m3 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(3)
                .setTestName("ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveInt32(14)
                        .build()
                ).build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicMessage.class, ProtoBeamBasicMessage.getDescriptor(), "protobeam_message"));
    }


    private void toBqOption(Pipeline pipeline) {
        int ix = 1;
        ProtoBeamOptionMessage m1 = ProtoBeamOptionMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("ProtoToBigQueryPipeline.toBqOption")
                .setOptionDescription("deprecated")
                .build();
        pipeline.apply(Create.of(m1))
                .apply(BigQueryWrite(ProtoBeamOptionMessage.class, ProtoBeamOptionMessage.getDescriptor(), "protobeam_option"));
    }

    private void toBqWkt(Pipeline pipeline) {
        int ix = 1;
        ProtoBeamWktMessage m1 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m2 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m3 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m4 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        pipeline.apply(Create.of(m1, m2, m3, m4))
                .apply(BigQueryWrite(ProtoBeamWktMessage.class, ProtoBeamWktMessage.getDescriptor(), "protobeam_wkt"));
    }


}
