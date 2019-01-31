package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import org.apache.beam.sdk.schemas.Schema;

public class SchemaProtoToBeamSQL {

    SchemaProtoContext context = new SchemaProtoContext();

    public Schema getSchema(Descriptors.Descriptor descriptor) {
        Schema.Builder schemaBuilder = new Schema.Builder();
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            schemaBuilder.addField(convertField(fieldDescriptor));
        }
        return schemaBuilder.build();
    }

    private Schema.FieldType convertMessage(Descriptors.FieldDescriptor fieldDescriptor) {
        Schema.FieldType fieldType = Schema.FieldType.row(getSchema(fieldDescriptor.getMessageType()));
        return fieldType;
    }

    private Schema.Field convertField(Descriptors.FieldDescriptor fieldDescriptor) {
        Schema.FieldType fieldType = null;
        if (context.isPrimitiveField(fieldDescriptor)) {
            fieldType = extractFieldType(fieldDescriptor);
        } else {
            fieldType = convertMessage(fieldDescriptor);
        }
        if (fieldDescriptor.isRepeated()) {
            fieldType = Schema.FieldType.array(fieldType);
        }
        if (fieldType == null) {
            throw new RuntimeException();
        }
        return Schema.Field.of(fieldDescriptor.getName(), fieldType);
    }

    private Schema.FieldType extractFieldType(Descriptors.FieldDescriptor fieldDescriptor) {

        if (context.isTimestamp(fieldDescriptor))
            return Schema.FieldType.DATETIME;
        if (context.isDecimal(fieldDescriptor))
            return Schema.FieldType.DECIMAL;
        Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();
        switch (fieldType) {
            case DOUBLE:
                return Schema.FieldType.DOUBLE;
            case FLOAT:
                return Schema.FieldType.FLOAT;
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                return Schema.FieldType.INT64;
            case INT32:
            case FIXED32:
            case UINT32:
            case SFIXED32:
            case SINT32:
                return Schema.FieldType.INT32;
            case BOOL:
                return Schema.FieldType.BOOLEAN;
            case STRING:
                return Schema.FieldType.STRING;
            case BYTES:
                return Schema.FieldType.BYTES;
            case ENUM:
                return Schema.FieldType.STRING;
            case MESSAGE:
                return null;
        }
        throw new RuntimeException("Field type not matched.");
    }
}
