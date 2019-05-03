package io.anemos.protobeam.convert;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.examples.ProtoBeamWktMessage;
import java.text.ParseException;
import org.junit.Before;
import org.junit.Test;

public class WktTest extends AbstractProtoBigQueryTest {

  private ProtoTableRowExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamWktMessage x = ProtoBeamWktMessage.newBuilder().build();
    plan = new ProtoTableRowExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void timestampFieldTest() throws ParseException {
    Timestamp timestamp = Timestamps.parse("2018-11-28T12:34:56.123456789Z");
    ProtoBeamWktMessage protoIn = ProtoBeamWktMessage.newBuilder().setTimestamp(timestamp).build();
    testPingPong(plan, protoIn);
    testWrapperPingPong(protoIn, "timestamp", Timestamps.toString(timestamp));
  }

  private void testWrapperPingPong(
      ProtoBeamWktMessage protoIn, String fieldName, Object expectedValue) {
    TableRow result = plan.convert(protoIn);
    assertEquals(expectedValue, result.get(fieldName));
    Message protoOut = plan.convertToProto(result);
    assertEquals(protoIn, protoOut);
  }

  @Test
  public void booleanValueTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableBool(BoolValue.newBuilder().setValue(true).build())
            .build();
    testWrapperPingPong(protoIn, "nullable_bool", true);
  }

  @Test
  public void stringValueTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableString(StringValue.newBuilder().setValue("fooBar1").build())
            .build();
    testWrapperPingPong(protoIn, "nullable_string", "fooBar1");
  }

  @Test
  public void int32ValueTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableInt32(Int32Value.newBuilder().setValue(42).build())
            .build();
    testWrapperPingPong(protoIn, "nullable_int32", 42);
  }

  @Test
  public void wktSchemaTest() {
    ProtoBeamWktMessage x = ProtoBeamWktMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "{fields=["
            + "{mode=REQUIRED, name=test_name, type=STRING}, "
            + "{mode=REQUIRED, name=test_index, type=INT64}, "
            + "{mode=NULLABLE, name=timestamp, type=TIMESTAMP}, "
            + "{mode=NULLABLE, name=nullable_string, type=STRING}, "
            + "{mode=NULLABLE, name=nullable_double, type=FLOAT64}, "
            + "{mode=NULLABLE, name=nullable_float, type=FLOAT64}, "
            + "{mode=NULLABLE, name=nullable_int32, type=INT64}, "
            + "{mode=NULLABLE, name=nullable_int64, type=INT64}, "
            + "{mode=NULLABLE, name=nullable_uint32, type=INT64}, "
            + "{mode=NULLABLE, name=nullable_uint64, type=INT64}, "
            + "{mode=NULLABLE, name=nullable_bool, type=BOOL}, "
            + "{mode=NULLABLE, name=nullable_bytes, type=BYTES}]}";
    SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
    assertEquals(modelRef, model.getSchema(descriptor).toString());

    String apiRef =
        "Schema{fields=["
            + "Field{name=test_name, type=STRING, mode=REQUIRED, description=null}, "
            + "Field{name=test_index, type=INTEGER, mode=REQUIRED, description=null}, "
            + "Field{name=timestamp, type=TIMESTAMP, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_string, type=STRING, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_double, type=FLOAT, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_float, type=FLOAT, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_int32, type=INTEGER, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_int64, type=INTEGER, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_uint32, type=INTEGER, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_uint64, type=INTEGER, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_bool, type=BOOLEAN, mode=NULLABLE, description=null}, "
            + "Field{name=nullable_bytes, type=BYTES, mode=NULLABLE, description=null}]}";
    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    assertEquals(apiRef, api.getSchema(descriptor).toString());
  }
}
