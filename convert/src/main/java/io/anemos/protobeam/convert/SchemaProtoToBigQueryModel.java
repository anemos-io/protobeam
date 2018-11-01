package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

public class SchemaProtoToBigQueryModel {

    public TableSchema getSchema(Descriptors.Descriptor descriptor) {
        List<TableFieldSchema> fieldSchemas = convertSchema(descriptor);
        return new TableSchema().setFields(fieldSchemas);
    }

    private List<TableFieldSchema> convertSchema(Descriptors.Descriptor descriptor) {
        List<TableFieldSchema> fieldSchemas = new ArrayList<>();
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            fieldSchemas.add(convertField(fieldDescriptor));
        }
        return fieldSchemas;
    }

    private TableFieldSchema convertField(Descriptors.FieldDescriptor fieldDescriptor) {
//        if (isNested(fieldDescriptor))
//            return getNestedSchema(fieldDescriptor);
        String bigQueryType = extractFieldType(fieldDescriptor);
        TableFieldSchema fieldSchema =
                new TableFieldSchema()
                        .setName(fieldDescriptor.getName())
                        .setType(bigQueryType);
        if (fieldDescriptor.isRepeated())
            fieldSchema.setMode("REPEATED");
        if ("STRUCT".equals(bigQueryType)) {
            fieldSchema.setFields(convertSchema(fieldDescriptor.getMessageType()));
        }
        return fieldSchema;
    }

    private String extractFieldType(Descriptors.FieldDescriptor fieldDescriptor) {
        if (isTimestamp(fieldDescriptor))
            return "TIMESTAMP";
        if (isDecimal(fieldDescriptor))
            return "FLOAT64";
        Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();
        switch (fieldType) {
            case DOUBLE:
            case FLOAT:
                return "FLOAT64";
            case INT64:
            case UINT64:
            case INT32:
            case FIXED64:
            case FIXED32:
            case UINT32:
            case SFIXED32:
            case SFIXED64:
            case SINT32:
            case SINT64:
                return "INT64";
            case BOOL:
                return "BOOL";
            case STRING:
                return "STRING";
            case BYTES:
                return "BYTES";
            case ENUM:
                return "STRING";
            case MESSAGE:
                return "STRUCT";
        }
        throw new RuntimeException("Field type not matched.");
    }

    private boolean isTimestamp(Descriptors.FieldDescriptor fieldDescriptor) {
        return (".google.protobuf.Timestamp".equals(fieldDescriptor.toProto().getTypeName()));
    }

    private boolean isDecimal(Descriptors.FieldDescriptor fieldDescriptor) {
        return ".bcl.Decimal".equals(fieldDescriptor.toProto().getTypeName());
    }

    private TableFieldSchema getNestedSchema(Descriptors.FieldDescriptor fieldDescriptor) {
//        if (isCampaignCategory(fieldDescriptor))
//            return getCampaignCategorySchema();
//        else if (isArticleReservation(fieldDescriptor))
//            return getArticleReservationSchema();
        throw new RuntimeException("No nested schema found for field " + fieldDescriptor.getName());
    }
}
