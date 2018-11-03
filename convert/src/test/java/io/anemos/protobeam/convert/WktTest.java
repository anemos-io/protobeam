package io.anemos.protobeam.convert;


import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.examples.ProtoBeamWktMessage;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;

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
    public void timestampFieldTest() throws ParseException {
        ProtoBeamWktMessage protoIn = ProtoBeamWktMessage.newBuilder()
                .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
                .build();
        testPingPong(plan, protoIn);
    }

}
