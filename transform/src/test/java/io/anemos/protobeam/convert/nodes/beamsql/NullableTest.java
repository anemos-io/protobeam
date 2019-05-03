package io.anemos.protobeam.convert.nodes.beamsql;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamWktMessage;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;

public class NullableTest extends AbstractProtoBeamSqlTest {

  private ProtoBeamSqlExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamWktMessage x = ProtoBeamWktMessage.newBuilder().build();
    plan = new ProtoBeamSqlExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoBeamSqlExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void testSchema() {
    ProtoBeamWktMessage x = ProtoBeamWktMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "Fields:\n"
            + "Field{name=test_name, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=test_index, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=timestamp, description=, type=FieldType{typeName=DATETIME, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_string, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_double, description=, type=FieldType{typeName=DOUBLE, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_float, description=, type=FieldType{typeName=FLOAT, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_int32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_int64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_uint32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_uint64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_bool, description=, type=FieldType{typeName=BOOLEAN, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n"
            + "Field{name=nullable_bytes, description=, type=FieldType{typeName=BYTES, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n";
    SchemaProtoToBeamSQL model = new SchemaProtoToBeamSQL();
    assertEquals(modelRef, model.getSchema(descriptor).toString());
  }

  private void testNullablePingPong(
      ProtoBeamWktMessage protoIn, String fieldName, Object expectedValue) {
    Row result = plan.convert(protoIn);
    assertEquals(expectedValue, result.getValue(fieldName));
    Message protoOut = plan.convertToProto(result);
    assertEquals(protoIn, protoOut);
  }

  @Test
  public void booleanFieldTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableBool(BoolValue.newBuilder().setValue(true).build())
            .build();
    testNullablePingPong(protoIn, "nullable_bool", true);
  }

  @Test
  public void stringFieldTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableString(StringValue.newBuilder().setValue("fooBar1").build())
            .build();
    testNullablePingPong(protoIn, "nullable_string", "fooBar1");
  }

  @Test
  public void twoNullableFieldsTest() {
    ProtoBeamWktMessage protoIn =
        ProtoBeamWktMessage.newBuilder()
            .setNullableString(StringValue.newBuilder().setValue("fooBar1").build())
            .setNullableBool(BoolValue.newBuilder().setValue(true).build())
            .build();
    Row result = plan.convert(protoIn);
    assertEquals("fooBar1", result.getValue("nullable_string"));
    assertEquals(true, result.getValue("nullable_bool"));
    Message protoOut = plan.convertToProto(result);
    assertEquals(protoIn, protoOut);
  }
}
