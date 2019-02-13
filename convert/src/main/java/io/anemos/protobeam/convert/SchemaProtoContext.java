package io.anemos.protobeam.convert;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.WrappersProto;
import io.anemos.Options;
import io.anemos.Meta;
import io.anemos.Rewrite;

import java.util.ArrayList;
import java.util.List;

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


}

