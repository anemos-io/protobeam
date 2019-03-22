package io.anemos.protobeam.convert.nodes.beamsql;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.convert.ProtoBeamSqlExecutionPlan;
import io.anemos.protobeam.convert.ProtoTableRowExecutionPlan;
import io.anemos.protobeam.examples.BigQueryOptionMessageTruncateMonthColumnRename;
import io.anemos.protobeam.examples.BigQueryRenameColumnMessage;
import io.anemos.protobeam.examples.RenameFieldMessage;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RenameFieldTest extends AbstractProtoBeamSqlTest {

    private ProtoBeamSqlExecutionPlan plan;

    @Test
    public void renameFieldTest() throws Exception {
        RenameFieldMessage protoIn = RenameFieldMessage.newBuilder()
                .setRenameField("testValue")
                .build();
        plan = new ProtoBeamSqlExecutionPlan(protoIn);
        byte[] so = SerializeTest.serializeToByteArray(plan);
        plan = (ProtoBeamSqlExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

        Row result = plan.convert(protoIn);
        assertEquals("testValue", result.getValue("renamedField"));
        Message protoOut = plan.convertToProto(result);
        assertTrue(protoOut.hasField(protoOut.getDescriptorForType().findFieldByName("renameField")));
        assertEquals(protoIn, protoOut);

    }


}
