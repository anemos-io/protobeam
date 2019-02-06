package io.anemos.protobeam.convert;


import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import io.anemos.protobeam.examples.ProtoBeamBasicNullablePrimitive;
import io.anemos.protobeam.examples.ProtoBeamBasicRepeatPrimitive;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NullableTest extends AbstractProtoBigQueryTest {

    private ProtoTableRowExecutionPlan plan;


    @Before
    public void setup() {
        ProtoBeamBasicNullablePrimitive x = ProtoBeamBasicNullablePrimitive.newBuilder()
                .build();
        plan = new ProtoTableRowExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    private void testNullablePingPong(ProtoBeamBasicNullablePrimitive protoIn, String fieldName, Object expectedValue) {
        TableRow result = plan.convert(protoIn);
        assertEquals(expectedValue, result.get(fieldName));
        Message protoOut = plan.convertToProto(result);
        assertEquals(protoIn, protoOut);
    }

    @Test
    public void booleanFieldTest() {
        ProtoBeamBasicNullablePrimitive protoIn = ProtoBeamBasicNullablePrimitive.newBuilder()
                .setNullableBool(BoolValue.newBuilder()
                        .setValue(true)
                        .build())
                .build();
        testNullablePingPong(protoIn, "nullable_bool", true);
    }


    @Test
    public void stringFieldTest() {
        ProtoBeamBasicNullablePrimitive protoIn = ProtoBeamBasicNullablePrimitive.newBuilder()
                .setNullableString(StringValue.newBuilder()
                        .setValue("fooBar1")
                        .build())
                .build();
        testNullablePingPong(protoIn, "nullable_string", "fooBar1");
    }

    @Test
    public void int32FieldTest() {
        ProtoBeamBasicNullablePrimitive protoIn = ProtoBeamBasicNullablePrimitive.newBuilder()
                .setNullableInt32(Int32Value.newBuilder()
                        .setValue(42)
                        .build())
                .build();
        testNullablePingPong(protoIn, "nullable_int32", 42);
    }

    @Test
    public void testNullableSchemaTest() {
        ProtoBeamBasicNullablePrimitive x = ProtoBeamBasicNullablePrimitive.newBuilder().build();
        Descriptors.Descriptor descriptor = x.getDescriptorForType();

        String modelRef = "{fields=[" +
                "{mode=NULLABLE, name=nullable_string, type=STRING}, " +
                "{mode=NULLABLE, name=nullable_double, type=FLOAT64}, " +
                "{mode=NULLABLE, name=nullable_float, type=FLOAT64}, " +
                "{mode=NULLABLE, name=nullable_int32, type=INT64}, " +
                "{mode=NULLABLE, name=nullable_int64, type=INT64}, " +
                "{mode=NULLABLE, name=nullable_uint32, type=INT64}, " +
                "{mode=NULLABLE, name=nullable_uint64, type=INT64}, " +
                "{mode=NULLABLE, name=nullable_bool, type=BOOL}, " +
                "{mode=NULLABLE, name=nullable_bytes, type=BYTES}]}";
        SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
        assertEquals(modelRef, model.getSchema(descriptor).toString());

        String apiRef = "Schema{fields=[" +
                "Field{name=nullable_string, type=STRING, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_double, type=FLOAT, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_float, type=FLOAT, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_int32, type=INTEGER, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_int64, type=INTEGER, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_uint32, type=INTEGER, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_uint64, type=INTEGER, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_bool, type=BOOLEAN, mode=NULLABLE, description=null}, " +
                "Field{name=nullable_bytes, type=BYTES, mode=NULLABLE, description=null}]}";
        SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
        assertEquals(apiRef, api.getSchema(descriptor).toString());
    }



}
