package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.examples.ProtoBeamBasicMessage;
import io.anemos.protobeam.examples.ProtoBeamBasicPrimitive;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessageTest extends AbstractProtoBigQueryTest {

    private ProtoTableRowExecutionPlan plan;


    @Before
    public void setup() {
        ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
        plan = new ProtoTableRowExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    //    @Test
//    @Ignore
    public void testSchema() {
        ProtoBeamBasicMessage x = ProtoBeamBasicMessage.newBuilder().build();
        Descriptors.Descriptor descriptor = x.getDescriptorForType();

        String modelRef = "{fields=[{name=payload, type=BYTES}, {fields=[{name=foo, type=STRING}], name=test_one, type=STRUCT}, {fields=[{name=bar, type=STRING}], mode=REPEATED, name=test_repeater, type=STRUCT}]}";
        SchemaProtoToBigQueryModel model = new SchemaProtoToBigQueryModel();
        assertEquals(modelRef, model.getSchema(descriptor).toString());

        String apiRef = "Schema{fields=[Field{name=payload, value=Type{value=BYTES, fields=null}, mode=null, description=null}, Field{name=test_one, value=Type{value=RECORD, fields=[Field{name=foo, value=Type{value=STRING, fields=null}, mode=null, description=null}]}, mode=null, description=null}, Field{name=test_repeater, value=Type{value=RECORD, fields=[Field{name=bar, value=Type{value=STRING, fields=null}, mode=null, description=null}]}, mode=REPEATED, description=null}]}";
        SchemaProtoToBigQueryApi api = new SchemaProtoToBigQueryApi();
        assertEquals(apiRef, api.getSchema(descriptor).toString());
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
