package io.anemos.protobeam.convert.nodes.tablerow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.anemos.protobeam.convert.SchemaProtoContext;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.Map;

public class PartitionColumnConvert extends AbstractMessageConvert<Object, TableRow, Map<String, Object>> {

    //TODO truncate inner class

    AbstractMessageConvert convert;
    SchemaProtoContext context;
    Descriptors.Descriptor descriptor;

    public PartitionColumnConvert(Descriptors.Descriptor descriptor, AbstractMessageConvert convert) {
        super(null); //TODO check
        this.convert = convert;
        this.descriptor = descriptor;
        this.context = new SchemaProtoContext();
    }

    @Override
    public Object fromProtoValue(Object in) {
        return in;
    }

    @Override
    public void fromProto(Message message, TableRow row) {
        convert.fromProto(message, row);

        String columnName = context.getTimePartitionColumnName(descriptor);
        String sourceFieldName = context.getBqTimePartitioningField(descriptor);
        Timestamp sourceTime = (Timestamp) message.getField(descriptor.findFieldByName(sourceFieldName));
        DateTime dateTime = new DateTime(Timestamps.toMillis(sourceTime), DateTimeZone.UTC);

        DateTime trunced;
        switch (context.getTimePartitioningTruncate(descriptor)) {
            case DAY:
                trunced = dateTime.dayOfMonth().roundFloorCopy();
                break;
            case MONTH:
                trunced = dateTime.monthOfYear().roundFloorCopy();
                break;
            case YEAR:
                trunced = dateTime.year().roundFloorCopy();
                break;
            default:
                throw new RuntimeException("TimePartitioningTruncate value: " + context.getTimePartitioningTruncate(descriptor) + "not supported.");
        }
        String date = trunced.toString(DateTimeFormat.forPattern("yyyy-MM-dd"));
        row.set(columnName, date);
    }

    @Override
    public void toProto(Map row, Message.Builder builder) {
        String columnName = context.getTimePartitionColumnName(descriptor);
        row.remove(columnName);

        convert.toProto(row, builder);
    }

}
