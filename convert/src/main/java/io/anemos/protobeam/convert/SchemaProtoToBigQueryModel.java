package io.anemos.protobeam.convert;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors;
import io.anemos.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaProtoToBigQueryModel {

    private SchemaProtoContext context = new SchemaProtoContext();

    public TableSchema getSchema(Descriptors.Descriptor descriptor) {
        List<TableFieldSchema> fieldSchemas = convertSchema(descriptor);
        return new TableSchema().setFields(fieldSchemas);
    }

    private List<TableFieldSchema> convertSchema(Descriptors.Descriptor descriptor) {
        List<TableFieldSchema> fieldSchemas = new ArrayList<>();
        FieldExplorer fieldExplorer = new FieldExplorer(descriptor);
        fieldExplorer.forEach(field -> {
            Descriptors.FieldDescriptor fieldDescriptor = field.fieldDescriptor;
            if (field.isOneOf) {
                fieldSchemas.add(convertField(fieldDescriptor, "NULLABLE"));
            } else if (context.flatten(fieldDescriptor)) {
                fieldDescriptor.getMessageType().getFields().forEach(subfieldDescriptor -> {
                    fieldSchemas.add(convertField(subfieldDescriptor));
                });
            } else {
                fieldSchemas.add(convertField(fieldDescriptor));
            }
        });
        return fieldSchemas;
    }

    private TableFieldSchema convertField(Descriptors.FieldDescriptor fieldDescriptor) {
        return convertField(fieldDescriptor, "REQUIRED");
    }

    private TableFieldSchema convertField(Descriptors.FieldDescriptor fieldDescriptor, String mode) {
        TableFieldSchema fieldSchema =
                new TableFieldSchema()
                        .setName(fieldDescriptor.getName());
        String bigQueryType = extractFieldType(fieldDescriptor);
        fieldSchema.setMode(mode);
        if ("STRUCT".equals(bigQueryType)) {
            fieldSchema.setMode("NULLABLE");
            if (context.isWrapper(fieldDescriptor)) {
                Descriptors.FieldDescriptor primitiveFieldDescriptor = fieldDescriptor.getMessageType().getFields().get(0);
                bigQueryType = extractFieldType(primitiveFieldDescriptor);
            } else {
                fieldSchema.setFields(convertSchema(fieldDescriptor.getMessageType()));
            }
        } else if ("TIMESTAMP".equals(bigQueryType)) {
            fieldSchema.setMode("NULLABLE");
        }

        fieldSchema.setType(bigQueryType);
        if (fieldDescriptor.isRepeated())
            fieldSchema.setMode("REPEATED");
        Map<Descriptors.FieldDescriptor, Object> allFields = fieldDescriptor.getOptions().getAllFields();
        if (allFields.size() > 0) {
            allFields.forEach((f, opt) -> {
                switch (f.getFullName()) {
                    case "google.protobuf.FieldOptions.deprecated":
                        Boolean deprecated = (Boolean) opt;
                        if (deprecated) {
                            String description = fieldSchema.getDescription();
                            if ((description == null) || description.length() == 0) {
                                description = "";
                            }
                            fieldSchema.setDescription("@deprecated\n" + description);
                        }
                        break;
                    case "anemos.field_meta":
                        Meta.FieldMeta meta = (Meta.FieldMeta) opt;
                        String description = fieldSchema.getDescription();
                        if ((description == null) || description.length() == 0) {
                            description = "";
                        }
                        fieldSchema.setDescription(description + meta.getDescription());
                        break;
                }
            });
        }

        return fieldSchema;
    }

    private String extractFieldType(Descriptors.FieldDescriptor fieldDescriptor) {
        if (context.isTimestamp(fieldDescriptor))
            return "TIMESTAMP";
        if (context.isDecimal(fieldDescriptor))
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


    private TableFieldSchema getNestedSchema(Descriptors.FieldDescriptor fieldDescriptor) {
        throw new RuntimeException("No nested schema found for field " + fieldDescriptor.getName());
    }
}
