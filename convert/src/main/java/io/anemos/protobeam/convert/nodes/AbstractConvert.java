package io.anemos.protobeam.convert.nodes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

public abstract class AbstractConvert<FIELD, IN, OUT> {
    protected Descriptors.FieldDescriptor fieldDescriptor;

    public AbstractConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    /**
     * Take a object that came from a proto field, and convert it into the format that the target
     * container expect
     *
     * @param in proto field object
     * @return container field object
     */
    public abstract Object convert(Object in);

    public abstract void convert(Message message, IN row);

    public abstract void convertToProto(Message.Builder builder, OUT row);

    public FIELD convertFrom(Object in) {
        return (FIELD) in;
    }
}