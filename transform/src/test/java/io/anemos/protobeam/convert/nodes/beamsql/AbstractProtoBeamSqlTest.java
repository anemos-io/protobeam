package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import org.apache.beam.sdk.values.Row;

import static org.junit.Assert.assertEquals;

public class AbstractProtoBeamSqlTest {

    protected void testPingPong(ProtoBeamSqlExecutionPlan plan, AbstractMessage protoIn) {
        Row result = plan.convert(protoIn);
        System.out.println(result.toString());
        Message protoOut = plan.convertToProto(result);
        System.out.println(protoOut.toString());
        assertEquals(protoIn, protoOut);
    }
}
