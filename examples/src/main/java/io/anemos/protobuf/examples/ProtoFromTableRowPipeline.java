package io.anemos.protobuf.examples;

import io.anemos.protobeam.examples.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;

public class ProtoFromTableRowPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ProtoFromTableRowPipeline().run();
    }

    private void run() {
        fromBqPrimitive();
        fromBqRepeat();
        fromBqMessage();
        fromBqOption();
        fromBqWkt();
        fromBqSpecial();
    }

    private void fromBqPrimitive() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamBasicPrimitive.class, ProtoBeamBasicPrimitive.getDescriptor(),
                "protobeam_primitive")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

    private void fromBqRepeat() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamBasicRepeatPrimitive.class, ProtoBeamBasicRepeatPrimitive.getDescriptor(),
                "protobeam_repeat")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

    private void fromBqMessage() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamBasicMessage.class, ProtoBeamBasicMessage.getDescriptor(),
                "protobeam_message")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

    private void fromBqOption() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamOptionMessage.class, ProtoBeamOptionMessage.getDescriptor(),
                "protobeam_option")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

    private void fromBqWkt() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamWktMessage.class, ProtoBeamWktMessage.getDescriptor(),
                "protobeam_wkt")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

    private void fromBqSpecial() {
        Pipeline pipeline = createPipeline();
        BigQueryRead(pipeline, ProtoBeamBasicSpecial.class, ProtoBeamBasicSpecial.getDescriptor(),
                "protobeam_special")
                .apply(ParDo.of(endMessage()))
        ;
        pipeline.run();
    }

}
