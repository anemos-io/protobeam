package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;

public class AbstractProtoBigQueryTest {

    protected void testPingPong(ProtoTableRowExecutionPlan plan, AbstractMessage protoIn) {
        TableRow result = plan.convert(protoIn);
        System.out.println(result.toString());
        Message protoOut = plan.convertToProto(result);
        System.out.println(protoOut.toString());
        assertEquals(protoIn, protoOut);
    }
}
