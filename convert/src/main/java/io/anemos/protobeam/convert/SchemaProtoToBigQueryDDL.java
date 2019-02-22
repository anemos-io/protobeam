package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.protobuf.Descriptors;

import java.util.List;

public class SchemaProtoToBigQueryDDL {

    private SchemaProtoToBigQueryModel modelConvert = new SchemaProtoToBigQueryModel();
    private DDL ddl = DDL.newBuilder();

    public DDL getSchema(Descriptors.Descriptor descriptor) {
        convertFieldList(modelConvert.getSchema(descriptor).getFields(), ddl.struct());
        return ddl;
    }

    private void convertFieldList(List<TableFieldSchema> tableFields, DDL.Struct struct) {
        for (TableFieldSchema tableField : tableFields) {
            struct.addColumn(convertField(tableField));
        }
    }

    private DDL.Column convertField(TableFieldSchema tableFieldSchema) {
        DDL.Column column = DDL.Column.newBuilder()
                .setName(tableFieldSchema.getName());

        if (tableFieldSchema.getType().equals("STRUCT")) {
            DDL.Struct struct = DDL.Struct.newBuilder();
            convertFieldList(tableFieldSchema.getFields(), struct);
            column.setSchema(struct);
        } else {
            extractFieldType(tableFieldSchema, column);
        }
        if (tableFieldSchema.getMode() != null)
            extractFieldMode(tableFieldSchema, column);
        if ((tableFieldSchema.getDescription() != null) && (tableFieldSchema.getDescription().length() > 0)) {
            column.setDescription(tableFieldSchema.getDescription());
        }

        return column;
    }

    private void extractFieldType(TableFieldSchema tableFieldSchema, DDL.Column column) {
        switch (tableFieldSchema.getType()) {
            case "INT64":
            case "BOOL":
            case "STRING":
            case "BYTES":
            case "TIMESTAMP":
            case "FLOAT64":
                column.setType(tableFieldSchema.getType());
                break;
            default:
                throw new RuntimeException("Cannot extractFieldType TableFieldSchema type " + tableFieldSchema.getType() + " to Field.Type");
        }
    }

    private void extractFieldMode(TableFieldSchema tableFieldSchema, DDL.Column column) {
        String mode = tableFieldSchema.getMode();
        switch (mode) {
            case "REPEATED":
                column.setArray(true);
                break;
            case "REQUIRED":
                column.setNullable(false);
                break;
            case "NULLABLE":
                column.setNullable(true);
                break;
            default:
                throw new RuntimeException("Cannot extractFieldType mode String to Field.Mode enum");

        }
    }

}
