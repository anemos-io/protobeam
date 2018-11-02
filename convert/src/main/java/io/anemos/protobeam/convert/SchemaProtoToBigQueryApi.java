package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

public class SchemaProtoToBigQueryApi {

    private SchemaProtoToBigQueryModel modelConvert = new SchemaProtoToBigQueryModel();

    public Schema getSchema(Descriptors.Descriptor descriptor) {
        return Schema.of(convertFieldList(modelConvert.getSchema(descriptor).getFields()));
    }

    private List<Field> convertFieldList(List<TableFieldSchema> tableFields) {
        List<Field> fields = new ArrayList<>();
        for (TableFieldSchema tableField : tableFields) {
            fields.add(convertField(tableField));
        }
        return fields;
    }

    private Field convertField(TableFieldSchema tableFieldSchema) {
        Field.Builder fieldBuilder = Field.newBuilder(tableFieldSchema.getName(), extractFieldType(tableFieldSchema));
        if (tableFieldSchema.getMode() != null)
            fieldBuilder.setMode(extractFieldMode(tableFieldSchema));
        if ((tableFieldSchema.getDescription() != null) && (tableFieldSchema.getDescription().length() > 0)) {
            fieldBuilder.setDescription(tableFieldSchema.getDescription());
        }
        return fieldBuilder.build();
    }

    private Field.Type extractFieldType(TableFieldSchema tableFieldSchema) {
        switch (tableFieldSchema.getType()) {
            case "FLOAT64":
                return Field.Type.floatingPoint();
            case "INT64":
                return Field.Type.integer();
            case "BOOL":
                return Field.Type.bool();
            case "STRING":
                return Field.Type.string();
            case "BYTES":
                return Field.Type.bytes();
            case "TIMESTAMP":
                return Field.Type.timestamp();
            case "STRUCT":
                return Field.Type.record(convertFieldList(tableFieldSchema.getFields()));
            default:
                throw new RuntimeException("Cannot extractFieldType TableFieldSchema type " + tableFieldSchema.getType() + " to Field.Type");
        }
    }

    private Field.Mode extractFieldMode(TableFieldSchema tableFieldSchema) {
        String mode = tableFieldSchema.getMode();
        switch (mode) {
            case "REPEATED":
                return Field.Mode.REPEATED;
            case "REQUIRED":
                return Field.Mode.REQUIRED;
            case "NULLABLE":
                return Field.Mode.NULLABLE;
            default:
                throw new RuntimeException("Cannot extractFieldType mode String to Field.Mode enum");

        }
    }

}
