package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.examples.BigQueryOptionMessage;
import io.anemos.protobeam.examples.BigQueryOptionMessageTruncateMonthColumnRename;
import io.anemos.protobeam.examples.BigQueryRenameColumnMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RenameFieldTest extends AbstractProtoBigQueryTest {

    @Test
    public void renameBigQueryColumnTest() throws Exception {
        BigQueryRenameColumnMessage protoIn = BigQueryRenameColumnMessage.newBuilder()
                .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
                .build();
        ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(protoIn);
        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

        TableRow result = plan.convert(protoIn);
        assertEquals("2018-11-28T12:34:56.123456789Z", result.get("CreationTime"));
        Message protoOut = plan.convertToProto(result);
        assertTrue(protoOut.hasField(protoOut.getDescriptorForType().findFieldByName("timestamp")));
        assertEquals(protoIn, protoOut);

    }

    @Test
    public void renameBigQueryColumnPartitionedTest() throws Exception {
        BigQueryOptionMessageTruncateMonthColumnRename protoIn = BigQueryOptionMessageTruncateMonthColumnRename.newBuilder()
                .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
                .build();
        ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(protoIn);
        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

        TableRow result = plan.convert(protoIn);
        assertEquals("2018-11-01", result.get("partition_month"));
        Message protoOut = plan.convertToProto(result);
        assertTrue(protoOut.hasField(protoOut.getDescriptorForType().findFieldByName("timestamp")));
        assertEquals(protoIn, protoOut);
    }


}
