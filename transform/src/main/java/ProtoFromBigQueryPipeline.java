import io.anemos.examples.Message;
import io.anemos.examples.MessagePrimitive;
import io.anemos.examples.MessageRepeatPrimitive;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;

public class ProtoFromBigQueryPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ProtoFromBigQueryPipeline().run();
    }

    private void run() {

        Pipeline pipeline = createPipeline();

        primitive(pipeline);
        repeat(pipeline);
        message(pipeline);

        pipeline.run();

    }

    private void primitive(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(MessagePrimitive.class, MessagePrimitive.getDescriptor(), "protobeam_primitive"))
                .apply(ParDo.of(end()))
        ;
    }

    private void repeat(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(MessageRepeatPrimitive.class, MessageRepeatPrimitive.getDescriptor(), "protobeam_repeat"))
                .apply(ParDo.of(end()))
        ;
    }

    private void message(Pipeline pipeline) {
        pipeline.apply(BigQueryRead(Message.class, Message.getDescriptor(), "protobeam_message"))
                .apply(ParDo.of(end()))
        ;
    }

}
