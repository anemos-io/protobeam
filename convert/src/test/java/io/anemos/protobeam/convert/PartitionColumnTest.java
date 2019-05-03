package io.anemos.protobeam.convert;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.examples.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionColumnTest extends AbstractProtoBigQueryTest {

  @Test
  public void partitionColumnNoTruncateTest() throws Exception {
    BigQueryOptionMessage optionMessage =
        BigQueryOptionMessage.newBuilder()
            .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
            .build();
    ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(optionMessage);
    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

    testPingPong(plan, optionMessage);
  }

  @Test
  public void partitionColumnTruncateDayTest() throws Exception {
    BigQueryOptionMessageTruncateDay optionMessageTruncated =
        BigQueryOptionMessageTruncateDay.newBuilder()
            .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
            .build();
    ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(optionMessageTruncated);
    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

    TableRow result = plan.convert(optionMessageTruncated);
    assertEquals("2018-11-28", result.get("partition_day"));
    Message protoOut = plan.convertToProto(result);
    assertEquals(optionMessageTruncated, protoOut);
  }

  @Test
  public void partitionColumnTruncateMonthTest() throws Exception {
    BigQueryOptionMessageTruncateMonth optionMessageTruncated =
        BigQueryOptionMessageTruncateMonth.newBuilder()
            .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
            .build();
    ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(optionMessageTruncated);
    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

    TableRow result = plan.convert(optionMessageTruncated);
    assertEquals("2018-11-01", result.get("partition_month"));
    Message protoOut = plan.convertToProto(result);
    assertEquals(optionMessageTruncated, protoOut);
  }

  @Test
  public void partitionColumnTruncateMonthCustomNameTest() throws Exception {
    BigQueryOptionMessageTruncateMonthCustomColumnName optionMessageTruncated =
        BigQueryOptionMessageTruncateMonthCustomColumnName.newBuilder()
            .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
            .build();
    ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(optionMessageTruncated);
    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

    TableRow result = plan.convert(optionMessageTruncated);
    assertEquals("2018-11-01", result.get("CreatedMonth"));
    Message protoOut = plan.convertToProto(result);
    assertEquals(optionMessageTruncated, protoOut);
  }

  @Test
  public void partitionColumnTruncateYearTest() throws Exception {
    BigQueryOptionMessageTruncateYear optionMessageTruncated =
        BigQueryOptionMessageTruncateYear.newBuilder()
            .setTimestamp(Timestamps.parse("2018-11-28T12:34:56.123456789Z"))
            .build();
    ProtoTableRowExecutionPlan plan = new ProtoTableRowExecutionPlan(optionMessageTruncated);
    byte[] so = SerializeTest.serializeToByteArray(plan);
    plan = (ProtoTableRowExecutionPlan) SerializeTest.deserializeFromByteArray(so, "");

    TableRow result = plan.convert(optionMessageTruncated);
    assertEquals("2018-01-01", result.get("partition_year"));
    Message protoOut = plan.convertToProto(result);
    assertEquals(optionMessageTruncated, protoOut);
  }
}
