package io.anemos.protobuf.examples;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.examples.*;
import io.anemos.protobeam.test.BytesGenerator;
import io.anemos.protobeam.test.TimestampGenerator;
import io.anemos.protobeam.transform.bigquery.ProtoBeamFactory;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.logging.Logger;

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
        toBqSpecial(pipeline);
        pipeline.run();
    }

    private void toBqrimitive(Pipeline pipeline) {
        ProtoBeamBasicPrimitive m1 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(1)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveString("abc")
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        ProtoBeamBasicPrimitive m2 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(2)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveDouble(42.13)
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        ProtoBeamBasicPrimitive m3 = ProtoBeamBasicPrimitive.newBuilder()
                .setTestIndex(3)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqrimitive")
                .setPrimitiveInt32(14)
                .setPrimitiveBytes(ByteString.copyFrom(bytesGenerator.nextBytes()))
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicPrimitive.class, ProtoBeamBasicPrimitive.getDescriptor(), "protobeam_primitive"));
    }

    private void toBqepeat(Pipeline pipeline) {
        ProtoBeamBasicRepeatPrimitive m1 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(1)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedDouble(12.34)
                .addRepeatedDouble(56.78)
                .build();
        ProtoBeamBasicRepeatPrimitive m2 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(2)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedInt32(12)
                .addRepeatedInt32(34)
                .build();
        ProtoBeamBasicRepeatPrimitive m3 = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .setTestIndex(3)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqepeat")
                .addRepeatedString("foo")
                .addRepeatedString("bar")
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicRepeatPrimitive.class, ProtoBeamBasicRepeatPrimitive.getDescriptor(), "protobeam_repeat"));
    }

    private void toBqMessage(Pipeline pipeline) {
        ProtoBeamBasicMessage m1 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(1)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveString("abc")
                        .build())
                .build();
        ProtoBeamBasicMessage m2 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(2)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveDouble(42.13)
                        .build())
                .build();
        ProtoBeamBasicMessage m3 = ProtoBeamBasicMessage.newBuilder()
                .setTestIndex(3)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqMessage")
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveInt32(14)
                        .build())
                .addRepeatedMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveString("row 3.1")
                        .build())
                .addRepeatedMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveString("row 3.2")
                        .build())
                .build();
        pipeline.apply(Create.of(m1, m2, m3))
                .apply(BigQueryWrite(ProtoBeamBasicMessage.class, ProtoBeamBasicMessage.getDescriptor(), "protobeam_message"));
    }


    private void toBqOption(Pipeline pipeline) {
        int ix = 1;
        ProtoBeamOptionMessage m1 = ProtoBeamOptionMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqOption")
                .setOptionDescription("deprecated")
                .build();
        pipeline.apply(Create.of(m1))
                .apply(BigQueryWrite(ProtoBeamOptionMessage.class, ProtoBeamOptionMessage.getDescriptor(), "protobeam_option"));
    }

    private void toBqWkt(Pipeline pipeline) {
        int ix = 1;
        ProtoBeamWktMessage m1 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m2 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m3 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        ProtoBeamWktMessage m4 = ProtoBeamWktMessage.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqWkt")
                .setTimestamp(tsGenerator.nextTimestamp())
                .build();
        pipeline.apply(Create.of(m1, m2, m3, m4))
                .apply(BigQueryWrite(ProtoBeamWktMessage.class, ProtoBeamWktMessage.getDescriptor(), "protobeam_wkt"));
    }

    private void toBqSpecial(Pipeline pipeline) {
        int ix = 1;
        ProtoBeamBasicSpecial m1 = ProtoBeamBasicSpecial.newBuilder()
                .setTestIndex(ix++)
                .setTestName("io.anemos.protobuf.examples.ProtoToBigQueryPipeline.toBqSpecial")
                .setSpecialEnum(ProtoBeamBasicSpecial.EnumSpecial.FOO)
                .addRepeatedEnum(ProtoBeamBasicSpecial.EnumSpecial.BAR)
                .addRepeatedEnum(ProtoBeamBasicSpecial.EnumSpecial.FOO)
                .build();
        pipeline.apply(Create.of(m1))
                .apply(BigQueryWrite(ProtoBeamBasicSpecial.class, ProtoBeamBasicSpecial.getDescriptor(), "protobeam_special"));
    }


    public static class DemoPipelineBase implements Serializable {

        private static final String GOOGLE_PROJECT_ID = "GOOGLE_PROJECT_ID";
        private static final String BQ_OUT_DATASET = "BQ_OUT_DATASET";
        private static final String BQ_GCS_TEMP = "BQ_GCS_TEMP";
        private static final String RUNNER = "RUNNER";
        private static Logger LOG = Logger.getLogger(DemoPipelineBase.class.toString());

        private void fail(String message) {
            throw new RuntimeException(message);
        }

        private String env(String key) {
            String value = System.getenv(key);
            if ((value == null) || (value.length() == 0)) {
                fail("Environment variable [" + key + "] is set.");
            }
            return value;
        }


        private PipelineOptions createDirect() {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
            DirectTestOptions options = PipelineOptionsFactory.as(DirectTestOptions.class);
            options.setGcpTempLocation(env(BQ_GCS_TEMP));
            options.setTempLocation(env(BQ_GCS_TEMP));
            options.setProject(env(GOOGLE_PROJECT_ID));
            options.setRunner(DirectRunner.class);
            return options;
        }

        private PipelineOptions createDataflow() {
            DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
            options.setStreaming(false);
            options.setProject(env(GOOGLE_PROJECT_ID));
            options.setGcpTempLocation(env(BQ_GCS_TEMP));
            options.setTempLocation(env(BQ_GCS_TEMP));
            options.setRunner(DataflowRunner.class);
            return options;
        }

        protected Pipeline createPipeline() {
            if ("Dataflow".equals(env(RUNNER))) {
                return Pipeline.create(createDataflow());
            }
            return Pipeline.create(createDirect());
        }

        protected DoFn<Message, String> endMessage() {
            return new DoFn<Message, String>() {

                @DoFn.ProcessElement
                public void processElement(ProcessContext c) {
                    LOG.info(c.element().toString());
                }
            };
        }

        protected DoFn<TableRow, String> endTableRow() {
            return new DoFn<TableRow, String>() {

                @DoFn.ProcessElement
                public void processElement(ProcessContext c) {
                    LOG.info(c.element().toString());
                }
            };
        }

        protected <M extends Message> BigQueryIO.TypedRead<M> BigQueryTypedRead(Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {
            ProtoBeamFactory factory = new ProtoBeamFactory(messageClass, descriptor);
            return BigQueryIO.read(factory.getBigQueryReadFormatFunction())
                    .from(env(BQ_OUT_DATASET) + "." + name)
                    .withCoder(ProtoCoder.of(messageClass));
        }

        protected <M extends Message> PCollection<M> BigQueryRead(Pipeline pipeline, Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {
            ProtoBeamFactory factory = new ProtoBeamFactory(messageClass, descriptor);
            PCollection<TableRow> in = pipeline.apply(BigQueryIO.readTableRows()
                    .from(env(BQ_OUT_DATASET) + "." + name));
            PCollection<M> typedIn = (PCollection<M>) in.apply(ParDo.of(factory.getBigQueryReadDoFn()));
            typedIn.setCoder(ProtoCoder.of(messageClass));
            return typedIn;
        }

        protected <M extends Message> BigQueryIO.Write<M> BigQueryWrite(Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {

            ProtoBeamFactory factory = new ProtoBeamFactory(messageClass, descriptor);
            //
            //        ValueProvider<String> gcs = new ValueProvider<String>() {
            //            @Override
            //            public String get() {
            //                return env(BQ_GCS_TEMP);
            //            }
            //
            //            @Override
            //            public boolean isAccessible() {
            //                return true;
            //            }
            //        };
            //
            return BigQueryIO
                    .write().withFormatFunction(factory.getBigQueryWriteFormatFunction())
                    .to(env(BQ_OUT_DATASET) + "." + name)
                    .withSchema(factory.getBigQuerySchema())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
            //.withCustomGcsTempLocation(gcs);
        }

    }
}
