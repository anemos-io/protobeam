package io.anemos.protobeam.convert;

import com.google.protobuf.Descriptors;
import io.anemos.protobeam.convert.nodes.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class ProtoBigQueryPlanner implements Serializable {

    private Descriptors.Descriptor descriptor;


    ProtoBigQueryPlanner(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }


    private AbstractConvert planField(Descriptors.FieldDescriptor fieldDescriptor) {
        Descriptors.FieldDescriptor.Type fieldType = fieldDescriptor.getType();
        switch (fieldType) {
            case BOOL:
                return new BooleanFieldConvert(fieldDescriptor);
            case INT64:
            case UINT64:
            case FIXED64:
            case SFIXED64:
            case SINT64:
                return new LongFieldConvert(fieldDescriptor);
            case INT32:
            case UINT32:
            case SFIXED32:
            case FIXED32:
            case SINT32:
                return new IntegerFieldConvert(fieldDescriptor);
            case DOUBLE:
                return new DoubleFieldConvert(fieldDescriptor);
            case FLOAT:
                return new FloatFieldConvert(fieldDescriptor);
            case BYTES:
                return new BytesFieldConvert(fieldDescriptor);
            case STRING:
                return new StringFieldConvert(fieldDescriptor);
            case ENUM:
                return new EnumConvert(fieldDescriptor);
            case MESSAGE:
                return planMessageField(fieldDescriptor);
        }
        throw new RuntimeException(fieldType.toString() + " is unsupported.");
    }

    private AbstractConvert planMessageField(Descriptors.FieldDescriptor fieldDescriptor) {
        if (WktTimestampConvert.isHandler(fieldDescriptor)) {
            return new WktTimestampConvert(fieldDescriptor);
        }
        return new MessageFieldConvert(fieldDescriptor, planMessage(fieldDescriptor, fieldDescriptor.getMessageType().getFields()));
    }

    private MessageConvert planMessage(Descriptors.FieldDescriptor fieldDescriptor, List<Descriptors.FieldDescriptor> fields) {
        List<AbstractConvert> list = new ArrayList<>();
        fields.forEach(fd -> {
            if (fd.isRepeated()) {
                list.add(new RepeatedConvert(fd, planField(fd)));
            } else {
                list.add(planField(fd));
            }
        });
        return new MessageConvert(null, list);
    }

    AbstractConvert createPlan() {
        return planMessage(null, descriptor.getFields());
    }

//    public ProtoBigQueryExecutionPlan create() {
//        return new ProtoBigQueryExecutionPlan(descriptor, planMessage(null, descriptor.getFields()));
//    }


}
