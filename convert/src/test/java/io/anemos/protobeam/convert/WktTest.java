package io.anemos.protobeam.convert;


import com.google.protobuf.Timestamp;
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
    public void timestampFieldTest() {
        MessageWkt protoIn = MessageWkt.newBuilder()
                .setTimestamp(Timestamp.newBuilder().build())
                .build();
        testPingPong(plan, protoIn);
    }

}
