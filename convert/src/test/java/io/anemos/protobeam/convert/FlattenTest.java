package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.examples.Meta;
import io.anemos.protobeam.examples.ToFlatten;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class FlattenTest extends AbstractProtoBigQueryTest {

    private ProtoTableRowExecutionPlan plan;

    @Before
    public void setup() {
        ToFlatten x = ToFlatten.newBuilder()
                .build();
        plan = new ProtoTableRowExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    @Test
    public void flattenMessageFieldTest() throws Exception {
        ToFlatten toFlatten = ToFlatten.newBuilder()
                .setTestString("fooBar1")
                .setTestInt32(42)
                .setMeta(
                    Meta.newBuilder()
                        .setM1("fooMeta")
                        .setM2(43)
                        .setM3(Timestamps.parse("2018-11-28T12:34:56.123456789Z")))
                .build();
        testFlattenPingPong(toFlatten);
    }

    public void testFlattenPingPong(ToFlatten protoIn) throws Exception {
        TableRow result = plan.convert(protoIn);
        assertEquals("fooBar1", result.get("test_string"));
//      assertEquals(42, result.get("test_int32"));
        assertEquals("fooMeta", result.get("m1"));
        assertEquals("43", result.get("m2"));
        assertEquals("2018-11-28T12:34:56.123456789Z", result.get("m3"));
        Message protoOut = plan.convertToProto(result);
        assertEquals(protoIn, protoOut);
    }

    @Test
    public void flattenSchemaTest() {
        ToFlatten x = ToFlatten.newBuilder().build();
        Descriptors.Descriptor descriptor = x.getDescriptorForType();
        String modelRef = "{fields=[" +
                "{mode=REQUIRED, name=test_string, type=STRING}, " +
                "{mode=REQUIRED, name=test_int32, type=INT64}, " +
                "{mode=REQUIRED, name=m1, type=STRING}, " +
                "{mode=REQUIRED, name=m2, type=INT64}, " +
                "{mode=NULLABLE, name=m3, type=TIMESTAMP}]}";
        SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
        assertEquals(modelRef, model.getSchema(descriptor).toString());

        String apiRef = "Schema{fields=[" +
                "Field{name=test_string, type=STRING, mode=REQUIRED, description=null}, " +
                "Field{name=test_int32, type=INTEGER, mode=REQUIRED, description=null}, " +
                "Field{name=m1, type=STRING, mode=REQUIRED, description=null}, " +
                "Field{name=m2, type=INTEGER, mode=REQUIRED, description=null}, " +
                "Field{name=m3, type=TIMESTAMP, mode=NULLABLE, description=null}]}";
        SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
        assertEquals(apiRef, api.getSchema(descriptor).toString());
    }




}
