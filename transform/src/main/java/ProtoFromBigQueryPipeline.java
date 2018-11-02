import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import io.anemos.protobeam.examples.ProtoBeamBasicRepeatPrimitive;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;

public class ProtoFromBigQueryPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ProtoFromBigQueryPipeline().run();
    }

    private void run() {
        Pipeline pipeline = createPipeline();
        fromBqPrimitive(pipeline);
        fromBqRepeat(pipeline);
        fromBqMessage(pipeline);
        pipeline.run();
    }

    private void fromBqPrimitive(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(ProtoBeamBasicPrimitive.class, ProtoBeamBasicPrimitive.getDescriptor(),
                "protobeam_primitive"))
                .apply(ParDo.of(end()))
        ;
    }

    private void fromBqRepeat(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(ProtoBeamBasicRepeatPrimitive.class, ProtoBeamBasicRepeatPrimitive.getDescriptor(),
                "protobeam_repeat"))
                .apply(ParDo.of(end()))
        ;
    }

    private void fromBqMessage(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(ProtoBeamBasicMessage.class, ProtoBeamBasicMessage.getDescriptor(),
                "protobeam_message"))
                .apply(ParDo.of(end()))
        ;
    }

}
