package io.anemos.protobeam.convert;


import io.anemos.examples.MessageRepeatPrimitive;
import org.junit.Before;
import org.junit.Test;

public class RepeatedTest extends AbstractProtoBigQueryTest {

    private ProtoBigQueryExecutionPlan plan;


    @Before
    public void setup() {
        MessageRepeatPrimitive x = MessageRepeatPrimitive.newBuilder()
                .build();
        plan = new ProtoBigQueryExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoBigQueryExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    @Test
    public void booleanFieldTest() {
        MessageRepeatPrimitive protoIn = MessageRepeatPrimitive.newBuilder()
                .addRepeatedBool(false)
                .addRepeatedBool(true)
                .build();
        testPingPong(plan, protoIn);
    }


    @Test
    public void stringFieldTest() {
        MessageRepeatPrimitive protoIn = MessageRepeatPrimitive.newBuilder()
                .addRepeatedString("fooBar1")
                .addRepeatedString("fooBar2")
                .build();
        testPingPong(plan, protoIn);
    }
}
