package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.AbstractMessageConvert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class ProtoConvertPlanner implements Serializable {

    private Descriptors.Descriptor descriptor;

    private ConvertNodeFactory nodeFactory;

    private SchemaProtoContext context = new SchemaProtoContext();


    ProtoConvertPlanner(Descriptors.Descriptor descriptor, ConvertNodeFactory factory) {
        this.nodeFactory = factory;
        this.descriptor = descriptor;
    }

    public String getNodeFactoryClassName() {
        return nodeFactory.getClass().getName();
    }

    private AbstractConvert planField(Descriptors.FieldDescriptor fieldDescriptor) {
        Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();
        switch (fieldType) {
            case BOOL:
                return nodeFactory.createBooleanFieldConvert(fieldDescriptor);
            case INT64:
            case UINT64:
            case FIXED64:
            case SFIXED64:
            case SINT64:
                return nodeFactory.createLongFieldConvert(fieldDescriptor);
            case INT32:
            case UINT32:
            case SFIXED32:
            case FIXED32:
            case SINT32:
                return nodeFactory.createIntegerFieldConvert(fieldDescriptor);
            case DOUBLE:
                return nodeFactory.createDoubleFieldConvert(fieldDescriptor);
            case FLOAT:
                return nodeFactory.createFloatFieldConvert(fieldDescriptor);
            case BYTES:
                return nodeFactory.createBytesFieldConvert(fieldDescriptor);
            case STRING:
                return nodeFactory.createStringFieldConvert(fieldDescriptor);
            case ENUM:
                return nodeFactory.createEnumFieldConvert(fieldDescriptor);
            case MESSAGE:
                return planMessageField(fieldDescriptor);
        }
        throw new RuntimeException(fieldType.toString() + " is unsupported.");
    }

    private AbstractConvert planMessageField(Descriptors.FieldDescriptor fieldDescriptor) {
        if (context.isTimestamp(fieldDescriptor)) {
            return nodeFactory.createWktTimestampFieldConvert(fieldDescriptor);
        }
        return nodeFactory.createMessageFieldConvert(fieldDescriptor, planMessage(fieldDescriptor, FieldExplorer.of(fieldDescriptor.getMessageType())));
    }

    private AbstractMessageConvert planMessage(Descriptors.FieldDescriptor fieldDescriptor, FieldExplorer fields) {
        List<AbstractConvert> list = new ArrayList<>();
        fields.forEach(field -> {
            Descriptors.FieldDescriptor fd = field.fieldDescriptor;
            if (fd.isRepeated()) {
                list.add(nodeFactory.createRepeatedFieldConvert(fd, planField(fd)));
            } else if (context.isNullable(fd)) {
                list.add(nodeFactory.createWktWrapperFieldConvert(fd));
            } else if (field.isOneOf) {
                list.add(nodeFactory.createNullableFieldConvert(fd, planField(fd)));
            } else {
                list.add(planField(fd));
            }
        });
        return nodeFactory.createMessageConvert(null, list);
    }

    AbstractConvert createPlan() {
        return planMessage(null, FieldExplorer.of(descriptor));
    }

    //TODO remove?
//    public ProtoTableRowExecutionPlan create() {
//        return new ProtoTableRowExecutionPlan(fieldDescriptor, planMessage(null, fieldDescriptor.getFields()));
//    }


}
