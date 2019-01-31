package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;

public class SchemaProtoContext {
    public boolean isPrimitiveField(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
            return isTimestamp(fieldDescriptor) || isDecimal(fieldDescriptor);
        }
        return true;
    }


    public boolean isTimestamp(Descriptors.FieldDescriptor fieldDescriptor) {
        return (".google.protobuf.Timestamp".equals(fieldDescriptor.toProto().getTypeName()));
    }

    public boolean isDecimal(Descriptors.FieldDescriptor fieldDescriptor) {
        return ".bcl.Decimal".equals(fieldDescriptor.toProto().getTypeName());
    }

}

