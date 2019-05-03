package io.anemos.protobuf.examples;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.anemos.protobeam.transform.bigquery.ProtoBeamFactory;
import java.io.Serializable;
import java.util.logging.Logger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class DemoPipelineBase implements Serializable {

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

  protected <M extends Message> BigQueryIO.TypedRead<M> BigQueryTypedRead(
      Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {
    ProtoBeamFactory factory = new ProtoBeamFactory(messageClass, descriptor);
    return BigQueryIO.read(factory.getBigQueryReadFormatFunction())
        .from(env(BQ_OUT_DATASET) + "." + name)
        .withCoder(ProtoCoder.of(messageClass));
  }

  protected <M extends Message> PCollection<M> BigQueryRead(
      Pipeline pipeline, Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {
    ProtoBeamFactory factory = new ProtoBeamFactory(messageClass, descriptor);
    PCollection<TableRow> in =
        pipeline.apply(BigQueryIO.readTableRows().from(env(BQ_OUT_DATASET) + "." + name));
    PCollection<M> typedIn = (PCollection<M>) in.apply(ParDo.of(factory.getBigQueryReadDoFn()));
    typedIn.setCoder(ProtoCoder.of(messageClass));
    return typedIn;
  }

  protected <M extends Message> BigQueryIO.Write<M> BigQueryWrite(
      Class<M> messageClass, Descriptors.Descriptor descriptor, String name) {

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
    return BigQueryIO.write()
        .withFormatFunction(factory.getBigQueryWriteFormatFunction())
        .to(env(BQ_OUT_DATASET) + "." + name)
        .withSchema(factory.getBigQuerySchema())
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    // .withCustomGcsTempLocation(gcs);
  }
}
