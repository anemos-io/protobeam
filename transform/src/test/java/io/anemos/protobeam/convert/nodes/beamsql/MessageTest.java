package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.SchemaProtoToBeamSQL;
import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

        String modelRef = "Fields:\n" +
                "Field{name=test_name, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=test_index, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=message, description=, type=FieldType{typeName=ROW, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=Fields:\n" +
                "Field{name=test_name, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=test_index, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_double, description=, type=FieldType{typeName=DOUBLE, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_float, description=, type=FieldType{typeName=FLOAT, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_int32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_int64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_uint32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_uint64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sint32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sint64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_fixed32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_fixed64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sfixed32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sfixed64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_bool, description=, type=FieldType{typeName=BOOLEAN, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_string, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_bytes, description=, type=FieldType{typeName=BYTES, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                ", metadata=null}, nullable=true}\n" +
                "Field{name=repeated_message, description=, type=FieldType{typeName=ARRAY, collectionElementType=FieldType{typeName=ROW, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=Fields:\n" +
                "Field{name=test_name, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=test_index, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_double, description=, type=FieldType{typeName=DOUBLE, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_float, description=, type=FieldType{typeName=FLOAT, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_int32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_int64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_uint32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_uint64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sint32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sint64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_fixed32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_fixed64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sfixed32, description=, type=FieldType{typeName=INT32, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_sfixed64, description=, type=FieldType{typeName=INT64, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_bool, description=, type=FieldType{typeName=BOOLEAN, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_string, description=, type=FieldType{typeName=STRING, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                "Field{name=primitive_bytes, description=, type=FieldType{typeName=BYTES, collectionElementType=null, collectionElementTypeNullable=null, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=false}\n" +
                ", metadata=null}, collectionElementTypeNullable=false, mapKeyType=null, mapValueType=null, mapValueTypeNullable=null, rowSchema=null, metadata=null}, nullable=true}\n";
        SchemaProtoToBeamSQL model = new SchemaProtoToBeamSQL();
        assertEquals(modelRef, model.getSchema(descriptor).toString());
    }


    @Test
    public void nestedMessageTest() {
        ProtoBeamBasicMessage protoIn = ProtoBeamBasicMessage.newBuilder()
                .setMessage(ProtoBeamBasicPrimitive.newBuilder()
                        .setPrimitiveBool(true)
                        .build())
                .build();
        testPingPong(plan, protoIn);
    }

    @Ignore("not supported yet")
    @Test
    public void repeatedNestedMessageTest() {
        ProtoBeamBasicPrimitive.Builder nested = ProtoBeamBasicPrimitive.newBuilder()
                .setPrimitiveBool(true);
        ProtoBeamBasicMessage protoIn = ProtoBeamBasicMessage.newBuilder()
                .addRepeatedMessage(nested.build())
                .addRepeatedMessage(nested.build())
                .build();
        testPingPong(plan, protoIn);
    }
}
