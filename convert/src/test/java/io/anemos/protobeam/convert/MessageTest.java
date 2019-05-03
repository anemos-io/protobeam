package io.anemos.protobeam.convert;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.junit.Before;
import org.junit.Test;

public class MessageTest extends AbstractProtoBigQueryTest {

  private ProtoTableRowExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
    plan = new ProtoTableRowExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void testSchema() {
    ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "{fields=["
            + "{mode=REQUIRED, name=test_name, type=STRING}, "
            + "{mode=REQUIRED, name=test_index, type=INT64}, "
            + "{fields=["
            + "{mode=REQUIRED, name=test_name, type=STRING}, "
            + "{mode=REQUIRED, name=test_index, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_double, type=FLOAT64}, "
            + "{mode=REQUIRED, name=primitive_float, type=FLOAT64}, "
            + "{mode=REQUIRED, name=primitive_int32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_int64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_uint32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_uint64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sint32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sint64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_fixed32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_fixed64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sfixed32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sfixed64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_bool, type=BOOL}, "
            + "{mode=REQUIRED, name=primitive_string, type=STRING}, "
            + "{mode=REQUIRED, name=primitive_bytes, type=BYTES}"
            + "], mode=NULLABLE, name=message, type=STRUCT}, "
            + "{fields=["
            + "{mode=REQUIRED, name=test_name, type=STRING}, "
            + "{mode=REQUIRED, name=test_index, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_double, type=FLOAT64}, "
            + "{mode=REQUIRED, name=primitive_float, type=FLOAT64}, "
            + "{mode=REQUIRED, name=primitive_int32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_int64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_uint32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_uint64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sint32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sint64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_fixed32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_fixed64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sfixed32, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_sfixed64, type=INT64}, "
            + "{mode=REQUIRED, name=primitive_bool, type=BOOL}, "
            + "{mode=REQUIRED, name=primitive_string, type=STRING}, "
            + "{mode=REQUIRED, name=primitive_bytes, type=BYTES}"
            + "], mode=REPEATED, name=repeated_message, type=STRUCT}"
            + "]}";
    SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
    assertEquals(modelRef, model.getSchema(descriptor).toString());

    String apiRef =
        "Schema{fields=[Field{name=test_name, type=STRING, mode=REQUIRED, description=null}, Field{name=test_index, type=INTEGER, mode=REQUIRED, description=null}, Field{name=message, type=RECORD, mode=NULLABLE, description=null}, Field{name=repeated_message, type=RECORD, mode=REPEATED, description=null}]}";
    SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
    assertEquals(apiRef, api.getSchema(descriptor).toString());
  }

  @Test
  public void nestedMessageTest() {
    ProtoBeamBasicMessage protoIn =
        ProtoBeamBasicMessage.newBuilder()
            .setMessage(ProtoBeamBasicPrimitive.newBuilder().setPrimitiveBool(true).build())
            .build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void repeatedNestedMessageTest() {
    ProtoBeamBasicPrimitive.Builder nested =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveBool(true);
    ProtoBeamBasicMessage protoIn =
        ProtoBeamBasicMessage.newBuilder()
            .addRepeatedMessage(nested.build())
            .addRepeatedMessage(nested.build())
            .build();
    testPingPong(plan, protoIn);
  }
}
