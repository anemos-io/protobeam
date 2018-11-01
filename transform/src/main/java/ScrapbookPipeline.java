import io.anemos.examples.MessagePrimitive;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;

public class ScrapbookPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ScrapbookPipeline().run();
    }

    public void run() {

        MessagePrimitive m1 = MessagePrimitive.newBuilder().build();
        MessagePrimitive m2 = MessagePrimitive.newBuilder().build();
        MessagePrimitive m3 = MessagePrimitive.newBuilder().build();

        Pipeline pipeline = createPipeline();

        pipeline.apply(Create.of(m1, m2, m3))
//                .apply(ParDo.of(protoToTableRow()))
                .apply(bigqueryIO(MessagePrimitive.class, MessagePrimitive.getDescriptor(),"test"));
        pipeline.run();

    }

}
