package io.anemos.protobeam.convert.nodes.beamsql;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamBasicRepeatPrimitive;
import org.junit.Before;
import org.junit.Test;

public class RepeatedTest extends AbstractProtoBeamSqlTest {

  private ProtoBeamSqlExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamBasicRepeatPrimitive x = ProtoBeamBasicRepeatPrimitive.newBuilder().build();
    plan = new ProtoBeamSqlExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoBeamSqlExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void testSchema() {
    ProtoBeamBasicRepeatPrimitive x = ProtoBeamBasicRepeatPrimitive.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "Fields:\n"
            + "Field{name=test_name, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=test_index, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_double, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=DOUBLE, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_float, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=FLOAT, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_int32, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_int64, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_uint32, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_uint64, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_sint32, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_sint64, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_fixed32, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_fixed64, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_sfixed32, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_sfixed64, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_bool, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=BOOLEAN, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_string, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n"
            + "Field{name=repeated_bytes, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=BYTES, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n";
    SchemaProtoToBeamSQL model = new SchemaProtoToBeamSQL();
    assertEquals(modelRef, model.getSchema(descriptor).toString());
  }

  @Test
  public void booleanFieldTest() {
    ProtoBeamBasicRepeatPrimitive protoIn =
        ProtoBeamBasicRepeatPrimitive.newBuilder()
            .addRepeatedBool(false)
            .addRepeatedBool(true)
            .build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void stringFieldTest() {
    ProtoBeamBasicRepeatPrimitive protoIn =
        ProtoBeamBasicRepeatPrimitive.newBuilder()
            .addRepeatedString("fooBar1")
            .addRepeatedString("fooBar2")
            .build();
    testPingPong(plan, protoIn);
  }
}
