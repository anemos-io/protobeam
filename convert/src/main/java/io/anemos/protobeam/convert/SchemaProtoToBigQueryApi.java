package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
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
        LegacySQLTypeName fieldType = extractFieldType(tableFieldSchema);
        Field.Builder fieldBuilder;
        if (fieldType.equals(LegacySQLTypeName.RECORD)) {
            FieldList fieldList = FieldList.of(convertFieldList(tableFieldSchema.getFields()));
            fieldBuilder = Field.newBuilder(tableFieldSchema.getName(), extractFieldType(tableFieldSchema), fieldList);
        } else {
            fieldBuilder = Field.newBuilder(tableFieldSchema.getName(), extractFieldType(tableFieldSchema));
        }
        if (tableFieldSchema.getMode() != null)
            fieldBuilder.setMode(extractFieldMode(tableFieldSchema));
        if ((tableFieldSchema.getDescription() != null) && (tableFieldSchema.getDescription().length() > 0)) {
            fieldBuilder.setDescription(tableFieldSchema.getDescription());
        }
        return fieldBuilder.build();
    }

    private LegacySQLTypeName extractFieldType(TableFieldSchema tableFieldSchema) {
        switch (tableFieldSchema.getType()) {
            case "FLOAT64":
                return LegacySQLTypeName.FLOAT;
            case "INT64":
                return LegacySQLTypeName.INTEGER;
            case "BOOL":
                return LegacySQLTypeName.BOOLEAN;
            case "STRING":
                return LegacySQLTypeName.STRING;
            case "BYTES":
                return LegacySQLTypeName.BYTES;
            case "TIMESTAMP":
                return LegacySQLTypeName.TIMESTAMP;
            case "STRUCT":
                return LegacySQLTypeName.RECORD;
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
