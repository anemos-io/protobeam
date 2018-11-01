package io.anemos.protobeam.convert;


import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.examples.MessageRepeatPrimitive;
import io.anemos.examples.MessageWkt;
import org.junit.Before;
import org.junit.Test;

public class WktTest extends AbstractProtoBigQueryTest {

    private ProtoBigQueryExecutionPlan plan;


    @Before
    public void setup() {
        MessageWkt x = MessageWkt.newBuilder()
                .build();
        plan = new ProtoBigQueryExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoBigQueryExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    @Test
    public void booleanFieldTest() {
        MessageWkt protoIn = MessageWkt.newBuilder()
                .setTimestamp(Timestamp.newBuilder().build())
                .build();
        testPingPong(plan, protoIn);
    }

}
