package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.schemas.Schema;

public class SchemaProtoToBeamSQL {

    SchemaProtoContext context = new SchemaProtoContext();

    public <T extends Message> Schema getSchema(Class<T> messageClass) {
        Descriptors.Descriptor descriptor = null;
        try {
            descriptor = (Descriptors.Descriptor) messageClass.getMethod("getDescriptor").invoke(null);
        } catch (Exception e) {
            throw new RuntimeException("ProtoBeam: Unable to get descriptor from Message. Maybe the class is not a Message?", e);
        }
        return getSchema(descriptor);
    }

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
        boolean isNullable = false;
        if (context.isPrimitiveField(fieldDescriptor)) {
            fieldType = extractFieldType(fieldDescriptor);
            if (context.isTimestamp(fieldDescriptor) || context.isDecimal(fieldDescriptor)) {
                isNullable = true;
            }
        } else if (context.isWrapper(fieldDescriptor)) {
            Descriptors.FieldDescriptor valueFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
            fieldType = extractFieldType(valueFieldDescriptor);
            return Schema.Field.nullable(fieldDescriptor.getName(), fieldType);
        } else {
            fieldType = convertMessage(fieldDescriptor);
            isNullable = true;
        }
        if (fieldDescriptor.isRepeated()) {
            fieldType = Schema.FieldType.array(fieldType);
        }
        if (fieldType == null) {
            throw new RuntimeException();
        }
        return Schema.Field.of(fieldDescriptor.getName(), fieldType).withNullable(isNullable);
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
