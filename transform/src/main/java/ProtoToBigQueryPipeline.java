import io.anemos.examples.Message;
import io.anemos.examples.MessagePrimitive;
import io.anemos.examples.MessageRepeatPrimitive;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;

public class ProtoToBigQueryPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ProtoToBigQueryPipeline().run();
    }

    public void run() {


        Pipeline pipeline = createPipeline();

        primitive(pipeline);
        repeat(pipeline);
        message(pipeline);

        pipeline.run();

    }

    private void primitive(Pipeline pipeline) {
        MessagePrimitive m1 = MessagePrimitive.newBuilder()
                .setPrimitiveString("abc")
                .build();
        MessagePrimitive m2 = MessagePrimitive.newBuilder()
                .setPrimitiveDouble(42.13)
                .build();
        MessagePrimitive m3 = MessagePrimitive.newBuilder()
                .setPrimitiveInt32(14)
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
//                .apply(ParDo.of(protoToTableRow()))
                .apply(bigqueryIO(MessagePrimitive.class, MessagePrimitive.getDescriptor(), "protobeam_primitive"));
    }

    private void repeat(Pipeline pipeline) {
        MessageRepeatPrimitive m1 = MessageRepeatPrimitive.newBuilder()
                .addRepeatedDouble(12.34)
                .addRepeatedDouble(56.78)
                .build();
        MessageRepeatPrimitive m2 = MessageRepeatPrimitive.newBuilder()
                .addRepeatedInt32(12)
                .addRepeatedInt32(34)
                .build();
        MessageRepeatPrimitive m3 = MessageRepeatPrimitive.newBuilder()
                .addRepeatedString("foo")
                .addRepeatedString("bar")
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
//                .apply(ParDo.of(protoToTableRow()))
                .apply(bigqueryIO(MessageRepeatPrimitive.class, MessageRepeatPrimitive.getDescriptor(), "protobeam_repeat"));
    }

    private void message(Pipeline pipeline) {
        Message m1 = Message.newBuilder()
                .setMessage(MessagePrimitive.newBuilder()
                        .setPrimitiveString("abc")
                        .build()
                ).build();
        Message m2 = Message.newBuilder()
                .setMessage(MessagePrimitive.newBuilder()
                        .setPrimitiveDouble(42.13)
                        .build()
                ).build();
        Message m3 = Message.newBuilder()
                .setMessage(MessagePrimitive.newBuilder()
                        .setPrimitiveInt32(14)
                        .build()
                ).build();
        pipeline.apply(Create.of(m1, m2, m3))
//                .apply(ParDo.of(protoToTableRow()))
                .apply(bigqueryIO(Message.class, Message.getDescriptor(), "protobeam_message"));
    }


}
