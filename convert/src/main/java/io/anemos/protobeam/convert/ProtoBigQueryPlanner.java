package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.AbstractConvert;
import io.anemos.protobeam.convert.nodes.MessageConvert;
import io.anemos.protobeam.convert.nodes.WktTimestampConvert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class ProtoBigQueryPlanner implements Serializable {

    private Descriptors.Descriptor descriptor;

    private ConvertNodeFactory nodeFactory;

    ProtoBigQueryPlanner(Descriptors.Descriptor descriptor) {
        this.nodeFactory = new BigQueryConvertNodeFactory();
        this.descriptor = descriptor;
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
        if (WktTimestampConvert.isHandler(fieldDescriptor)) {
            return nodeFactory.createWktTimestampFieldConvert(fieldDescriptor);
        }
        return nodeFactory.createMessageFieldConvert(fieldDescriptor, planMessage(fieldDescriptor, fieldDescriptor.getMessageType().getFields()));
    }

    private MessageConvert planMessage(Descriptors.FieldDescriptor fieldDescriptor, List<Descriptors.FieldDescriptor> fields) {
        List<AbstractConvert> list = new ArrayList<>();
        fields.forEach(fd -> {
            if (fd.isRepeated()) {
                list.add(nodeFactory.createRepeatedFieldConvert(fd, planField(fd)));
            } else {
                list.add(planField(fd));
            }
        });
        return nodeFactory.createMessageConvert(null, list);
    }

    AbstractConvert createPlan() {
        return planMessage(null, descriptor.getFields());
    }

//    public ProtoBigQueryExecutionPlan create() {
//        return new ProtoBigQueryExecutionPlan(descriptor, planMessage(null, descriptor.getFields()));
//    }


}
