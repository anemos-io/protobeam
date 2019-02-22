package io.anemos.protobeam.convert;


import com.google.protobuf.Descriptors;
import io.anemos.protobeam.examples.ProtoBeamBasicRepeatPrimitive;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RepeatedTest extends AbstractProtoBigQueryTest {

    private ProtoTableRowExecutionPlan plan;


    @Before
    public void setup() {
        ProtoBeamBasicRepeatPrimitive x = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .build();
        plan = new ProtoTableRowExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    @Test
    public void testSchema() {
        ProtoBeamBasicRepeatPrimitive x = ProtoBeamBasicRepeatPrimitive.newBuilder().build();
        Descriptors.Descriptor descriptor = x.getDescriptorForType();

        String modelRef = "{fields=[{mode=REQUIRED, name=test_name, type=STRING}, {mode=REQUIRED, name=test_index, type=INT64}, {mode=REPEATED, name=repeated_double, type=FLOAT64}, {mode=REPEATED, name=repeated_float, type=FLOAT64}, {mode=REPEATED, name=repeated_int32, type=INT64}, {mode=REPEATED, name=repeated_int64, type=INT64}, {mode=REPEATED, name=repeated_uint32, type=INT64}, {mode=REPEATED, name=repeated_uint64, type=INT64}, {mode=REPEATED, name=repeated_sint32, type=INT64}, {mode=REPEATED, name=repeated_sint64, type=INT64}, {mode=REPEATED, name=repeated_fixed32, type=INT64}, {mode=REPEATED, name=repeated_fixed64, type=INT64}, {mode=REPEATED, name=repeated_sfixed32, type=INT64}, {mode=REPEATED, name=repeated_sfixed64, type=INT64}, {mode=REPEATED, name=repeated_bool, type=BOOL}, {mode=REPEATED, name=repeated_string, type=STRING}, {mode=REPEATED, name=repeated_bytes, type=BYTES}]}";
        SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
        assertEquals(modelRef, model.getSchema(descriptor).toString());

        String apiRef = "Schema{fields=[Field{name=test_name, type=STRING, mode=REQUIRED, description=null}, Field{name=test_index, type=INTEGER, mode=REQUIRED, description=null}, Field{name=repeated_double, type=FLOAT, mode=REPEATED, description=null}, Field{name=repeated_float, type=FLOAT, mode=REPEATED, description=null}, Field{name=repeated_int32, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_int64, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_uint32, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_uint64, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_sint32, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_sint64, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_fixed32, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_fixed64, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_sfixed32, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_sfixed64, type=INTEGER, mode=REPEATED, description=null}, Field{name=repeated_bool, type=BOOLEAN, mode=REPEATED, description=null}, Field{name=repeated_string, type=STRING, mode=REPEATED, description=null}, Field{name=repeated_bytes, type=BYTES, mode=REPEATED, description=null}]}";
        SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
        assertEquals(apiRef, api.getSchema(descriptor).toString());

        String ddlRef = "CREATE TABLE `test` (\n" +
                "\t`test_name` STRING NOT NULL,\n" +
                "\t`test_index` INT64 NOT NULL,\n" +
                "\t`repeated_double` ARRAY<FLOAT64>,\n" +
                "\t`repeated_float` ARRAY<FLOAT64>,\n" +
                "\t`repeated_int32` ARRAY<INT64>,\n" +
                "\t`repeated_int64` ARRAY<INT64>,\n" +
                "\t`repeated_uint32` ARRAY<INT64>,\n" +
                "\t`repeated_uint64` ARRAY<INT64>,\n" +
                "\t`repeated_sint32` ARRAY<INT64>,\n" +
                "\t`repeated_sint64` ARRAY<INT64>,\n" +
                "\t`repeated_fixed32` ARRAY<INT64>,\n" +
                "\t`repeated_fixed64` ARRAY<INT64>,\n" +
                "\t`repeated_sfixed32` ARRAY<INT64>,\n" +
                "\t`repeated_sfixed64` ARRAY<INT64>,\n" +
                "\t`repeated_bool` ARRAY<BOOL>,\n" +
                "\t`repeated_bytes` ARRAY<BYTES>\n" +
                ")\n";
        SchemaProtoToBigQueryDDL ddl = new SchemaProtoToBigQueryDDL();
        assertEquals(ddlRef, ddl.getSchema(descriptor).setTableName("test").toString());
    }

    @Test
    public void booleanFieldTest() {
        ProtoBeamBasicRepeatPrimitive protoIn = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .addRepeatedBool(false)
                .addRepeatedBool(true)
                .build();
        testPingPong(plan, protoIn);
    }


    @Test
    public void stringFieldTest() {
        ProtoBeamBasicRepeatPrimitive protoIn = ProtoBeamBasicRepeatPrimitive.newBuilder()
                .addRepeatedString("fooBar1")
                .addRepeatedString("fooBar2")
                .build();
        testPingPong(plan, protoIn);
    }
}
