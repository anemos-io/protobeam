package io.anemos.protobeam.convert;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.WrappersProto;
import io.anemos.Bigquery;
import io.anemos.Meta;
import io.anemos.Options;
import io.anemos.Rewrite;

import java.util.ArrayList;
import java.util.List;

import static io.anemos.Bigquery.BigQueryMessageOptions.AddTimePartitioningTruncateColumn.NO_TRUNCATE;

public class SchemaProtoContext {

    private List<String> wrapperDescriptors;

    public SchemaProtoContext() {
        this.wrapperDescriptors  = new ArrayList<>();
        WrappersProto.getDescriptor().getMessageTypes().forEach( d -> {
            this.wrapperDescriptors.add(d.getFullName());
        });
    }

    public boolean isPrimitiveField(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
            return isTimestamp(fieldDescriptor) || isDecimal(fieldDescriptor);
        }
        return true;
    }

    public boolean isWrapper(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
            return false;
        }
        return wrapperDescriptors.contains(fieldDescriptor.getMessageType().getFullName());
    }


    public boolean isTimestamp(Descriptors.FieldDescriptor fieldDescriptor) {
        return (".google.protobuf.Timestamp".equals(fieldDescriptor.toProto().getTypeName()));
    }

    public boolean isDecimal(Descriptors.FieldDescriptor fieldDescriptor) {
        return ".bcl.Decimal".equals(fieldDescriptor.toProto().getTypeName());
    }

    public boolean flatten(Descriptors.FieldDescriptor fieldDescriptor) {
        DescriptorProtos.FieldOptions fieldOptions = fieldDescriptor.getOptions();
        if (fieldDescriptor.getOptions().hasExtension(Options.fieldRewrite)) {
            Descriptors.FieldDescriptor flattenFieldDescriptor = Rewrite.FieldRewriteRule.getDescriptor().findFieldByName("flatten");
            return (Boolean) fieldDescriptor.getOptions().getExtension(Options.fieldRewrite).getField(flattenFieldDescriptor);
        }
        return false;
    }

    public boolean hasBqTimePartitioningField(Descriptors.Descriptor messageDescriptor) {
        if (messageDescriptor.getOptions().hasExtension(Options.messageBigquery)) {
            Bigquery.BigQueryMessageOptions bigQueryMessageOptions =  messageDescriptor.getOptions().getExtension(Options.messageBigquery);
            return bigQueryMessageOptions.hasField(bigQueryMessageOptions.getDescriptorForType().findFieldByName("time_partitioning_field"));
        }
        return false;
    }

    public String getBqTimePartitioningField(Descriptors.Descriptor messageDescriptor) {
        Bigquery.BigQueryMessageOptions bigQueryMessageOptions =  messageDescriptor.getOptions().getExtension(Options.messageBigquery);
        return (String) bigQueryMessageOptions.getField(bigQueryMessageOptions.getDescriptorForType().findFieldByName("time_partitioning_field"));}

    public boolean hasNonDefaultTimePartitioningTruncate(Descriptors.Descriptor messageDescriptor) {
        if (messageDescriptor.getOptions().hasExtension(Options.messageBigquery)) {
            Bigquery.BigQueryMessageOptions bigQueryMessageOptions = messageDescriptor.getOptions().getExtension(Options.messageBigquery);
            boolean hasField = bigQueryMessageOptions.hasField(bigQueryMessageOptions.getDescriptorForType().findFieldByName("add_time_partitioning_truncate_column"));
            if (hasField) {
                Bigquery.BigQueryMessageOptions.AddTimePartitioningTruncateColumn truncate = bigQueryMessageOptions.getAddTimePartitioningTruncateColumn();
                return !truncate.equals(NO_TRUNCATE);
            }
        }
        return false;
    }

    public Bigquery.BigQueryMessageOptions.AddTimePartitioningTruncateColumn getTimePartitioningTruncate(Descriptors.Descriptor messageDescriptor) {
        Bigquery.BigQueryMessageOptions bigQueryMessageOptions = messageDescriptor.getOptions().getExtension(Options.messageBigquery);
        return bigQueryMessageOptions.getAddTimePartitioningTruncateColumn();
    }

    public String getTimePartitionColumnName(Descriptors.Descriptor messageDescriptor) {
        Bigquery.BigQueryMessageOptions bigQueryMessageOptions = messageDescriptor.getOptions().getExtension(Options.messageBigquery);
        if (bigQueryMessageOptions.hasField(bigQueryMessageOptions.getDescriptorForType().findFieldByName("time_partitioning_truncate_column_name"))) {
            return bigQueryMessageOptions.getTimePartitioningTruncateColumnName();
        }

        Bigquery.BigQueryMessageOptions.AddTimePartitioningTruncateColumn truncateField = bigQueryMessageOptions.getAddTimePartitioningTruncateColumn();
        switch (truncateField) {
            case NO_TRUNCATE:
                return getBqTimePartitioningField(messageDescriptor);
            case DAY:
                return "partition_day";
            case MONTH:
                return "partition_month";
            case YEAR:
                return "partition_year";
        }
        throw new RuntimeException(truncateField.name() + " not supported");
    }
}

