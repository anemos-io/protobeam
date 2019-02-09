package io.anemos.protobuf.examples;

import io.anemos.protobeam.examples.*;
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
        fromBqOption(pipeline);
        fromBqWkt(pipeline);
        fromBqSpecial(pipeline);
        pipeline.run();
    }

    private void fromBqPrimitive(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamBasicPrimitive.class, ProtoBeamBasicPrimitive.getDescriptor(),
                "protobeam_primitive"))
                .apply(ParDo.of(endMessage()))
        ;
    }

    private void fromBqRepeat(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamBasicRepeatPrimitive.class, ProtoBeamBasicRepeatPrimitive.getDescriptor(),
                "protobeam_repeat"))
                .apply(ParDo.of(endMessage()))
        ;
    }

    private void fromBqMessage(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamBasicMessage.class, ProtoBeamBasicMessage.getDescriptor(),
                "protobeam_message"))
                .apply(ParDo.of(endMessage()))
        ;
    }

    private void fromBqOption(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamOptionMessage.class, ProtoBeamOptionMessage.getDescriptor(),
                "protobeam_option"))
                .apply(ParDo.of(endMessage()))
        ;
    }

    private void fromBqWkt(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamWktMessage.class, ProtoBeamWktMessage.getDescriptor(),
                "protobeam_wkt"))
                .apply(ParDo.of(endMessage()))
        ;
    }

    private void fromBqSpecial(Pipeline pipeline) {
        pipeline.apply(BigQueryTypedRead(ProtoBeamBasicSpecial.class, ProtoBeamBasicSpecial.getDescriptor(),
                "protobeam_special"))
                .apply(ParDo.of(endMessage()))
        ;
    }

}
