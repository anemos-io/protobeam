package io.anemos.protobeam.convert.nodes.beamsql;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.junit.Before;
import org.junit.Test;

public class PrimitiveTest extends AbstractProtoBeamSqlTest {

  private ProtoBeamSqlExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamBasicPrimitive x = ProtoBeamBasicPrimitive.newBuilder().build();
    plan = new ProtoBeamSqlExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoBeamSqlExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void testSchema() {
    ProtoBeamBasicPrimitive x = ProtoBeamBasicPrimitive.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "Fields:\n"
            + "Field{name=test_name, description=, type=FieldType{typeName=STRING, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=test_index, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_double, description=, type=FieldType{typeName=DOUBLE, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_float, description=, type=FieldType{typeName=FLOAT, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_int32, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_int64, description=, type=FieldType{typeName=INT64, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_uint32, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_uint64, description=, type=FieldType{typeName=INT64, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_sint32, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_sint64, description=, type=FieldType{typeName=INT64, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_fixed32, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_fixed64, description=, type=FieldType{typeName=INT64, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_sfixed32, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_sfixed64, description=, type=FieldType{typeName=INT64, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_bool, description=, type=FieldType{typeName=BOOLEAN, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_string, description=, type=FieldType{typeName=STRING, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=primitive_bytes, description=, type=FieldType{typeName=BYTES, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n";
    SchemaProtoToBeamSQL model = new SchemaProtoToBeamSQL();
    assertEquals(modelRef, model.getSchema(descriptor).toString());
  }

  @Test
  public void booleanFieldTest() {
    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveBool(true).build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void doubleFieldTest() {
    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveDouble(12.45).build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void intFieldTest() {
    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveInt32(42).build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void longFieldTest() {
    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveInt64(42).build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void stringFieldTest() {
    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveString("fooBar").build();
    testPingPong(plan, protoIn);
  }

  @Test
  public void bytesFieldTest() {
    byte[] buffer = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};

    ProtoBeamBasicPrimitive protoIn =
        ProtoBeamBasicPrimitive.newBuilder().setPrimitiveBytes(ByteString.copyFrom(buffer)).build();
    testPingPong(plan, protoIn);
  }
}
