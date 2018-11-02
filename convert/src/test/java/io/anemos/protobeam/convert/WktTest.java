package io.anemos.protobeam.convert;


import com.google.protobuf.Timestamp;
import io.anemos.protobeam.examples.ProtoBeamWktMessage;
import org.junit.Before;
import org.junit.Test;

public class WktTest extends AbstractProtoBigQueryTest {

    private ProtoBigQueryExecutionPlan plan;


    @Before
    public void setup() {
        ProtoBeamWktMessage x = ProtoBeamWktMessage.newBuilder()
                .build();
        plan = new ProtoBigQueryExecutionPlan(x);

        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoBigQueryExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");
    }

    @Test
    public void timestampFieldTest() {
        ProtoBeamWktMessage protoIn = ProtoBeamWktMessage.newBuilder()
                .setTimestamp(Timestamp.newBuilder().build())
                .build();
        testPingPong(plan, protoIn);
    }

}
