package io.anemos.protobeam.convert.nodes.beamsql;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MessageTest extends AbstractProtoBeamSqlTest {

  private ProtoBeamSqlExecutionPlan plan;

  @Before
  public void setup() {
    ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
    plan = new ProtoBeamSqlExecutionPlan(x);

    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoBeamSqlExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
  }

  @Test
  public void testSchema() {
    ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
    Descriptors.Descriptor descriptor = x.getDescriptorForType();

    String modelRef =
        "Fields:\n"
            + "Field{name=test_name, description=, type=FieldType{typeName=STRING, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=test_index, description=, type=FieldType{typeName=INT32, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + "Field{name=message, description=, type=FieldType{typeName=ROW, nullable=true, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=Fields:\n"
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
            + "Field{name=primitive_bytes, description=, type=FieldType{typeName=BYTES, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + ", metadata={}}}\n"
            + "Field{name=repeated_message, description=, type=FieldType{typeName=ARRAY, nullable=true, logicalType=null, collectionElementType=FieldType{typeName=ROW, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=Fields:\n"
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
            + "Field{name=primitive_bytes, description=, type=FieldType{typeName=BYTES, nullable=false, logicalType=null, collectionElementType=null, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n"
            + ", metadata={}}, mapKeyType=null, mapValueType=null, rowSchema=null, metadata={}}}\n";
    SchemaProtoToBeamSQL model = new SchemaProtoToBeamSQL();
    assertEquals(modelRef, model.getSchema(descriptor).toString());
  }

  @Test
  public void nestedMessageTest() {
    ProtoBeamBasicMessage protoIn =
        ProtoBeamBasicMessage.newBuilder()
            .setMessage(ProtoBeamBasicPrimitive.newBuilder().setPrimitiveBool(true).build())
            .build();
    testPingPong(plan, protoIn);
  }

  @Ignore("not supported yet")
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
