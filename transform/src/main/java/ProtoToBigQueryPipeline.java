import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import io.anemos.protobeam.examples.ProtoBeamBasicRepeatPrimitive;
import io.anemos.protobeam.examples.ProtoBeamOptionMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;

public class ProtoToBigQueryPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ProtoToBigQueryPipeline().run();
    }

    private void run() {
        Pipeline pipeline = createPipeline();
        toBqrimitive(pipeline);
        toBqepeat(pipeline);
        toBqMessage(pipeline);
        toBqOption(pipeline);
        pipeline.run();
    }

    private void toBqrimitive(Pipeline pipeline) {
        ProtoBeamBasicPrimitive m1 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(1)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveString("abc")
                .build();
        ProtoBeamBasicPrimitive m2 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(2)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveDouble(42.13)
                .build();
        ProtoBeamBasicPrimitive m3 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(3)
                .setTestName("ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveInt32(14)
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
        ProtoBeamOptionMessage m1 = ProtoBeamOptionMessage.newBuilder()
                .setTestIndex(1)
                .setTestName("ProtoToBigQueryPipeline.toBqOption")
                .setOptionDescription("deprecated")
                .build();
        pipeline.apply(Create.of(m1))
                .apply(BigQueryWrite(ProtoBeamOptionMessage.class, ProtoBeamOptionMessage.getDescriptor(), "protobeam_option"));
    }


}
