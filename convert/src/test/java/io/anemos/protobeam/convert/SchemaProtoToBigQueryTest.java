package io.anemos.protobeam.convert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.protobuf.Descriptors;
import io.anemos.protobeam.examples.*;
import org.junit.Test;

public class SchemaProtoToBigQueryTest extends AbstractProtoBigQueryTest {

  @Test
  public void testSchemaForSchemaTest() {
    ProtoBeamOptionMessage x = ProtoBeamOptionMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "{fields=[{mode=REQUIRED, name=test_name, type=STRING}, "
            + "{mode=REQUIRED, name=test_index, type=INT64}, {description=@deprecated\n"
            + ", mode=REQUIRED, name=option_deprecated, type=STRING}, "
            + "{description=A very detailed description, mode=REQUIRED, name=option_description, type=STRING}]}";
    SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
    assertEquals(modelRef, model.getSchema(descriptor).toString());

    String apiRef =
        "Schema{fields=["
            + "Field{name=test_name, type=STRING, mode=REQUIRED, description=null}, "
            + "Field{name=test_index, type=INTEGER, mode=REQUIRED, description=null}, "
            + "Field{name=option_deprecated, type=STRING, mode=REQUIRED, description=@deprecated\n"
            + "}, "
            + "Field{name=option_description, type=STRING, mode=REQUIRED, description=A very detailed description}]}";
    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    assertEquals(apiRef, api.getSchema(descriptor).toString());
    assertNull(api.getTimePartitioning(descriptor));
  }

  @Test
  public void testTableDefinitionWithTimePartitioning() {
    BigQueryOptionMessage message = BigQueryOptionMessage.getDefaultInstance();
    Descriptors.Descriptor descriptor = message.getDescriptorForType();

    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    TimePartitioning timePartitioning = api.getTimePartitioning(descriptor);
    assertEquals("timestamp", timePartitioning.getField());
  }

  @Test
  public void testTableDefinitionWithTimePartitioningAndTruncate() {
    BigQueryOptionMessageTruncateMonth message =
        BigQueryOptionMessageTruncateMonth.getDefaultInstance();
    Descriptors.Descriptor descriptor = message.getDescriptorForType();

    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    TimePartitioning timePartitioning = api.getTimePartitioning(descriptor);
    assertEquals("partition_month", timePartitioning.getField());
  }

  @Test
  public void testTableDefinitionWithTimePartitioningAndTruncateCustumColumnName() {
    BigQueryOptionMessageTruncateMonthCustomColumnName message =
        BigQueryOptionMessageTruncateMonthCustomColumnName.getDefaultInstance();
    Descriptors.Descriptor descriptor = message.getDescriptorForType();

    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    TimePartitioning timePartitioning = api.getTimePartitioning(descriptor);
    assertEquals("CreatedMonth", timePartitioning.getField());
  }

  @Test
  public void testRenameTo() {
    RenameFieldMessage renameFieldMessage = RenameFieldMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = renameFieldMessage.getDescriptorForType();

    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    String expected =
        "Schema{fields=[Field{name=renamedField, type=STRING, mode=REQUIRED, description=null}]}";
    assertEquals(expected, api.getSchema(descriptor).toString());
  }
}
